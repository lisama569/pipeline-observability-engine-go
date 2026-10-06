package pipelineobs

import (
	"fmt"
	"time"
)

// AlertSeverity 告警级别。
type AlertSeverity string

const (
	SeverityWarning  AlertSeverity = "warning"
	SeverityCritical AlertSeverity = "critical"
)

// Cause 是告警根因类型（同根因共享去重/恢复状态机）。
type Cause string

const (
	CauseStale   Cause = "data_stale"   // 数据滞后
	CauseStuck   Cause = "task_stuck"   // 任务卡住（无进展）
	CauseTimeout Cause = "task_timeout" // 任务运行超时
	CauseFailed  Cause = "task_failed"  // 任务执行失败
)

// AlertState 告警（事件单）生命周期状态。
type AlertState string

const (
	StateFiring    AlertState = "firing"    // 触发中（条件成立）
	StateRecovered AlertState = "recovered" // 已恢复
)

// Alert 是一条可观测告警 / 事件单。
type Alert struct {
	// Key 为告警根因的唯一键。
	Key                string
	Pipeline, Operator string
	Cause              Cause
	Severity           AlertSeverity
	Message            string

	State AlertState

	FirstFiredAt time.Time // 首次触发时间（处理时间）
	LastFiredAt  time.Time // 最近一次条件成立时间
	RecoveredAt  time.Time // 恢复时间（未恢复为零值）
	FireCount    int       // 历史触发轮次（恢复后重新恶化会 +1）
}

// evaluateAlerts 在状态更新后推进全部告警状态机。
//
// touchedOps 是本次接入涉及的 "pipeline|operator" 集合：数据滞后只评估
// 这些算子（新鲜度随接入变化）；任务卡住/超时/失败扫描全部在途/已完成任务
// （时间观察推进可能让任意在途任务卡住/超时）。
//
// 返回本次“新产生（且未被静默）”的告警与“新恢复”的告警。
// 同一 firing 轮次无论条件成立多少次、是否跨越 SuppressWindow，
// 都只产生一条有效告警；恢复后再次恶化 FireCount+1 并重新产生告警。
// 该结论只依赖已持久化的状态与传入的 now，因此时钟回拨/重启后可复现。
func (e *Engine) evaluateAlerts(now time.Time, touchedOps map[string]bool) (fired []Alert, recovered []Alert) {
	s := e.state

	// 静默到期在一次评估内统一生效：随后的通知闸门立即看到“覆盖解除”，
	// 持续中的问题因而能在同一次评估重新报警。
	e.sweepExpiredSilencesLocked(now)

	// 1) 数据滞后：针对本次触及的算子。
	for opKey := range touchedOps {
		pipe, op := splitOperatorKey(opKey)
		last, seen := s.LastIngest[opKey]
		// 边界口径：滞后差值“等于”阈值即判滞后（>=）。
		stale := seen && now.Sub(last) >= e.cfg.StalenessThreshold
		var msg string
		if seen {
			msg = fmt.Sprintf("数据滞后：距最近有效接入 %s 达到阈值 %s",
				now.Sub(last).Truncate(time.Millisecond), e.cfg.StalenessThreshold)
		}
		a, r := e.applyCondition(now, pipe, op, alertKey(pipe, op, CauseStale),
			CauseStale, SeverityWarning, stale, msg)
		fired = append(fired, a...)
		recovered = append(recovered, r...)
	}

	// 2) 任务卡住 / 超时 / 执行失败：扫描任务。
	//
	// 卡住根因键按算子共享，必须先在一次扫描内按算子聚合“任一在途任务卡住”，
	// 再对每个算子只推进一次状态机，避免多任务处理顺序导致同一次评估内
	// firing->recovered->firing 抖动。任务永不物理删除，因此全部结束后该算子
	// 聚合值为 false，既有卡住事件单据此恢复。
	stuckByOp := map[string]bool{}
	stuckTaskByOp := map[string]*taskState{}
	for _, t := range s.Tasks {
		opK := operatorKey(t.Pipeline, t.Operator)
		stuckByOp[opK] = false // 确保所有出现过任务的算子都参与（含恢复评估）
		if t.Running && e.cfg.TaskStuckThreshold > 0 && !t.LastProgressAt.IsZero() &&
			now.Sub(t.LastProgressAt) >= e.cfg.TaskStuckThreshold {
			stuckByOp[opK] = true
			if stuckTaskByOp[opK] == nil || t.TaskID < stuckTaskByOp[opK].TaskID {
				stuckTaskByOp[opK] = t // 取 TaskID 最小者，使告警文案确定
			}
		}
	}
	for opK, stuck := range stuckByOp {
		pipe, op := splitOperatorKey(opK)
		msg := ""
		if stuck {
			t := stuckTaskByOp[opK]
			msg = fmt.Sprintf("任务 %s 卡住无进展：距最近事件 %s ≥ 阈值 %s（任务未结束）",
				t.TaskID, now.Sub(t.LastProgressAt).Truncate(time.Millisecond), e.cfg.TaskStuckThreshold)
		}
		a2, r2 := e.applyCondition(now, pipe, op, alertKey(pipe, op, CauseStuck),
			CauseStuck, SeverityCritical, stuck, msg)
		fired = append(fired, a2...)
		recovered = append(recovered, r2...)
	}

	for _, t := range s.Tasks {
		wm := s.Watermarks[operatorKey(t.Pipeline, t.Operator)]

		// 超时：任务仍在运行，且事件时间水位 - 任务开始事件时间 >= 阈值。
		// 用事件时间水位评估，处理时钟回拨不改变结论。
		timedOut := t.Running && !wm.IsZero() && wm.Sub(t.StartedAt) >= e.cfg.TaskTimeout
		a, r := e.applyCondition(now, t.Pipeline, t.Operator,
			alertKey(t.Pipeline, t.Operator, CauseTimeout), CauseTimeout, SeverityCritical, timedOut,
			fmt.Sprintf("任务 %s 运行超时：事件时间水位 %s，运行跨度 ≥ %s",
				t.TaskID, wm.Format(time.RFC3339), e.cfg.TaskTimeout))
		fired = append(fired, a...)
		recovered = append(recovered, r...)
		// 超时未再成立时复位任务上的留痕标记，使后续运行可再次判定。
		if !timedOut {
			t.TimeoutAlerted = false
		}

		// 失败：任务以 Failed=true 结束。任务事件不可变，每个任务实例
		// 用独立的失败根因键（含 TaskID），天然“一轮一条”，重启恢复后
		// Notified 仍为 true，不会重复刷屏；下一个失败任务是新键，可再次触发。
		// 每次扫描都经过通知闸门：静默解除/到期后，仍 firing 的失败也重新报警。
		if !t.Running && t.Failed {
			if !t.FailAlerted {
				t.FailAlerted = true
			}
			if a := e.fireOnce(now, t, fmt.Sprintf("任务 %s 执行失败：%s", t.TaskID, t.ErrorMessage)); a != nil {
				fired = append(fired, *a)
			}
		}
	}
	return fired, recovered
}

// applyCondition 按布尔条件推进某根因键的告警状态机。
func (e *Engine) applyCondition(now time.Time, pipe, op, key string, cause Cause, sev AlertSeverity, cond bool, msg string) (fired []Alert, recovered []Alert) {
	s := e.state
	inc, exists := s.Incidents[key]

	if cond {
		if !exists || inc.State == StateRecovered {
			// 首次恶化，或恢复后再次恶化：开启新的 firing 轮次。
			if !exists {
				inc = &incidentState{Key: key, Pipeline: pipe, Operator: op, Cause: cause, Severity: sev}
				s.Incidents[key] = inc
			}
			inc.State = StateFiring
			inc.FirstFiredAt = now
			inc.RecoveredAt = time.Time{}
			inc.FireCount++
			inc.Notified = false
			// 新一轮开始时按当前静默覆盖情况置位 Muted，避免“恢复-再恶化”
			// 沿用上一轮的静默态。
			inc.Muted = e.isMutedLocked(pipe, op, cause, key)
		}
		inc.LastFiredAt = now
		if a := e.notifyGateLocked(inc, now, msg); a != nil {
			fired = append(fired, *a)
		}
		return fired, nil
	}

	// 条件不成立：firing -> recovered（恢复）。
	if exists && inc.State == StateFiring {
		inc.State = StateRecovered
		inc.RecoveredAt = now
		inc.Notified = false
		inc.Muted = false
		recovered = append(recovered, inc.toAlert("条件已恢复"))
	}
	return nil, recovered
}

// notifyGateLocked 是统一的“对外通知闸门”，按静默覆盖的跳变驱动唯一口径：
//   - 未被任何规则覆盖：本轮若尚未通知，则产生唯一一条告警并置 Notified；
//   - 被覆盖：只置 Muted，绝不产生告警（内部状态机照常 firing 并持久化）；
//   - 覆盖发生 true->false 跳变（静默到期/手动解除）：复位 Notified，
//     于是持续中的问题立刻重新产生一条告警，而不因其“以前报过一次”而静音；
//   - 覆盖 false->true 跳变：若此前已通知，保留 Notified，静默期间不补发。
//
// 多种范围同时命中只影响“是否被覆盖”这一布尔结论，作用口径唯一确定。
func (e *Engine) notifyGateLocked(inc *incidentState, now time.Time, msg string) *Alert {
	muted := e.isMutedLocked(inc.Pipeline, inc.Operator, inc.Cause, inc.Key)
	switch {
	case muted && !inc.Muted:
		e.logf("ALERT MUTED key=%s cause=%s rules=[%s]",
			inc.Key, inc.Cause, e.describeSilenceCoverage(inc.Pipeline, inc.Operator, inc.Cause, inc.Key))
	case !muted && inc.Muted:
		e.logf("ALERT UNMUTED key=%s cause=%s at=%s", inc.Key, inc.Cause, now.Format(time.RFC3339))
		inc.Notified = false // 覆盖解除：问题仍在 -> 允许重新报警
	}
	inc.Muted = muted
	if muted || inc.Notified {
		return nil
	}
	inc.Notified = true
	a := inc.toAlert(msg)
	return &a
}

// fireOnce 处理一次性的任务失败告警（每个 TaskID 一个根因键）。
// 通知经统一闸门：静默期内不发；静默解除/到期且事件单仍 firing 时重新发一条。
func (e *Engine) fireOnce(now time.Time, t *taskState, msg string) *Alert {
	s := e.state
	key := failureAlertKey(t.Pipeline, t.Operator, t.TaskID)
	inc, exists := s.Incidents[key]
	if !exists {
		inc = &incidentState{
			Key: key, Pipeline: t.Pipeline, Operator: t.Operator, Cause: CauseFailed,
			Severity: SeverityCritical, State: StateFiring,
			FirstFiredAt: now, LastFiredAt: now, FireCount: 1,
		}
		s.Incidents[key] = inc
	}
	inc.LastFiredAt = now
	// 首次建档时按当前静默覆盖置位 Muted；已存在则由闸门识别跳变。
	if !exists {
		inc.Muted = e.isMutedLocked(t.Pipeline, t.Operator, CauseFailed, key)
	}
	return e.notifyGateLocked(inc, now, msg)
}

// toAlert 把持久化事件单转为对外告警视图。
func (inc *incidentState) toAlert(msg string) Alert {
	return Alert{
		Key:          inc.Key,
		Pipeline:     inc.Pipeline,
		Operator:     inc.Operator,
		Cause:        inc.Cause,
		Severity:     inc.Severity,
		Message:      msg,
		State:        inc.State,
		FirstFiredAt: inc.FirstFiredAt,
		LastFiredAt:  inc.LastFiredAt,
		RecoveredAt:  inc.RecoveredAt,
		FireCount:    inc.FireCount,
	}
}

// splitOperatorKey 拆分 operatorKey，未知格式时整体当作 pipeline。
func splitOperatorKey(k string) (pipeline, operator string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '|' {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

// failureAlertKey 生成任务失败专属根因键（含 TaskID）。
func failureAlertKey(pipeline, operator, taskID string) string {
	return alertKey(pipeline, operator, CauseFailed) + "|" + taskID
}
