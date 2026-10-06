package pipelineobs

import (
	"fmt"
	"sort"
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
	CauseTimeout Cause = "task_timeout" // 任务运行超时
	CauseFailed  Cause = "task_failed"  // 任务执行失败
	CauseStuck   Cause = "task_stuck"   // 任务卡死（运行中但算子完全无进展）
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
// 这些算子（新鲜度随接入变化）；任务超时/失败扫描全部在途/已完成任务
// （水位推进可能让任意在途任务超时）。
//
// 返回本次“新产生（且未被静默）”的告警与“新恢复”的告警。
// 同一 firing 轮次无论条件成立多少次、是否跨越 SuppressWindow，
// 都只产生一条有效告警；恢复后再次恶化 FireCount+1 并重新产生告警。
// 该结论只依赖已持久化的状态与传入的 now，因此时钟回拨/重启后可复现。
func (e *Engine) evaluateAlerts(now time.Time, touchedOps map[string]bool) (fired []Alert, recovered []Alert) {
	s := e.state

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

	// 2) 任务超时 / 执行失败：扫描任务。
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
		// FailAlerted 仅在真正对外通知后置位：若失败发生时处于静默期，
		// 后续评估会重试，静默解除后仍可补发这一次失败。
		if !t.Running && t.Failed && !t.FailAlerted {
			if a := e.fireOnce(now, t, fmt.Sprintf("任务 %s 执行失败：%s", t.TaskID, t.ErrorMessage)); a != nil {
				fired = append(fired, *a)
			}
		}
	}

	// 3) 任务卡死（无进展）：任务仍在运行，且其算子距最近一次有效事件
	//    （单调有效处理时间）达到 StuckThreshold。与数据滞后（无任何接入，
	//    不要求有在途任务）、任务超时（事件时间水位口径）严格区分：
	//    即使完全没有新事件、只有真实时钟前进，ObserveTime 也会判出。
	//    StuckThreshold <= 0 时该判定关闭（兼容旧配置零值）。
	if e.cfg.StuckThreshold > 0 {
		// 同一算子可能有多个在途任务：先按算子聚合，结论对每个算子唯一，
		// 与 map 遍历顺序无关。
		type stuckInfo struct {
			stuck   bool
			taskIDs []string
			maxIdle time.Duration
		}
		byOp := map[string]*stuckInfo{}
		for _, t := range s.Tasks {
			if !t.Running || t.LastActivityAt.IsZero() {
				continue
			}
			opK := operatorKey(t.Pipeline, t.Operator)
			info := byOp[opK]
			if info == nil {
				info = &stuckInfo{}
				byOp[opK] = info
			}
			idle := now.Sub(t.LastActivityAt)
			// 边界口径：无进展时长“等于”阈值即判卡死（>=）。
			if idle >= e.cfg.StuckThreshold {
				info.stuck = true
				info.taskIDs = append(info.taskIDs, t.TaskID)
				if idle > info.maxIdle {
					info.maxIdle = idle
				}
			}
		}
		// 也要让“曾在途、现已全部结束”的算子从 firing 恢复：对出现过
		// 卡死事件单的算子统一评估。卡死事件单键即 alertKey(pipe,op,CauseStuck)。
		for _, inc := range s.Incidents {
			if inc.Cause != CauseStuck {
				continue
			}
			opK := operatorKey(inc.Pipeline, inc.Operator)
			if _, ok := byOp[opK]; !ok {
				byOp[opK] = &stuckInfo{}
			}
		}
		opKeys := make([]string, 0, len(byOp))
		for k := range byOp {
			opKeys = append(opKeys, k)
		}
		sort.Strings(opKeys)
		for _, opK := range opKeys {
			info := byOp[opK]
			pipe, op := splitOperatorKey(opK)
			sort.Strings(info.taskIDs)
			msg := fmt.Sprintf("任务卡死：算子 %d 个在途任务无进展（任务 %v），距最近有效事件 %s 达到阈值 %s",
				len(info.taskIDs), info.taskIDs, info.maxIdle.Truncate(time.Millisecond), e.cfg.StuckThreshold)
			a, r := e.applyCondition(now, pipe, op, alertKey(pipe, op, CauseStuck),
				CauseStuck, SeverityCritical, info.stuck, msg)
			fired = append(fired, a...)
			recovered = append(recovered, r...)
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
		}
		inc.LastFiredAt = now
		// 抑制口径：同一 firing 轮次只通知一次；SuppressWindow 不另开
		// 新轮次（状态机未恢复就不算新根因），因此窗口内外都不刷屏。
		// 例外：若上一轮评估时本事件单处于静默覆盖下（Silenced=true），
		// 静默解除（到期/手动移除）后只要条件仍成立，必须补发一条——
		// 不能因为“以前报过一次”或“静默期错过了”就一直不出声。
		_, silenced := e.silencedRule(inc, now)
		if !silenced && (!inc.Notified || inc.Silenced) {
			inc.Notified = true
			inc.Silenced = false
			fired = append(fired, inc.toAlert(msg))
		} else {
			inc.Silenced = silenced
		}
		return fired, nil
	}

	// 条件不成立：firing -> recovered（恢复）。
	if exists && inc.State == StateFiring {
		inc.State = StateRecovered
		inc.RecoveredAt = now
		inc.Notified = false
		recovered = append(recovered, inc.toAlert("条件已恢复"))
	}
	return nil, recovered
}

// fireOnce 处理一次性的任务失败告警（每个 TaskID 一个根因键）。
// 真正对外通知时才置 Notified 与任务上的 FailAlerted；静默期内不通知，
// 解除后由后续评估补发。
func (e *Engine) fireOnce(now time.Time, t *taskState, msg string) *Alert {
	s := e.state
	key := failureAlertKey(t.Pipeline, t.Operator, t.TaskID)
	inc, exists := s.Incidents[key]
	if !exists {
		inc = &incidentState{
			Key: key, Pipeline: t.Pipeline, Operator: t.Operator, Cause: CauseFailed,
			Severity: SeverityCritical, State: StateFiring,
			FirstFiredAt: now, LastFiredAt: now, FireCount: 1,
			TaskID: t.TaskID,
		}
		s.Incidents[key] = inc
	}
	inc.LastFiredAt = now
	_, silenced := e.silencedRule(inc, now)
	if silenced {
		inc.Silenced = true
		return nil
	}
	// 重启后 Notified 已持久化为 true：不重复发出；但静默刚解除且本轮
	// 尚未有效通知过时，允许补发一次。
	if inc.Notified && !inc.Silenced {
		return nil
	}
	inc.Notified = true
	inc.Silenced = false
	t.FailAlerted = true
	a := inc.toAlert(msg)
	return &a
}

// silencedRule 返回当前时刻覆盖该事件单、且最具体的生效静默规则 ID。
// 多条规则同时命中时效果为“并集”（任一命中即静默），返回哪一条由
// 具体程度（specificity）降序、规则 ID 升序唯一确定，便于观测与解释。
// 到期的规则在此惰性清理；边界口径：now >= Until 即失效。
func (e *Engine) silencedRule(inc *incidentState, now time.Time) (string, bool) {
	s := e.state
	bestID := ""
	bestScore := -1
	for id, sil := range s.Silences {
		if !sil.Until.IsZero() && !now.Before(sil.Until) {
			delete(s.Silences, id)
			continue
		}
		if !sil.matches(inc) {
			continue
		}
		if score := sil.specificity(); score > bestScore || (score == bestScore && id < bestID) {
			bestID, bestScore = id, score
		}
	}
	return bestID, bestScore >= 0
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
