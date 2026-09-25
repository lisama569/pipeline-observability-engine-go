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
		if !t.Running && t.Failed && !t.FailAlerted {
			t.FailAlerted = true
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
		}
		inc.LastFiredAt = now
		// 抑制口径：同一 firing 轮次只通知一次；SuppressWindow 不另开
		// 新轮次（状态机未恢复就不算新根因），因此窗口内外都不刷屏。
		if !inc.Notified && !e.isSilenced(key, now) {
			inc.Notified = true
			fired = append(fired, inc.toAlert(msg))
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
	// 重启后 Notified 已持久化为 true：不重复发出。
	if inc.Notified || e.isSilenced(key, now) {
		return nil
	}
	inc.Notified = true
	a := inc.toAlert(msg)
	return &a
}

// isSilenced 判断告警键在 now 时刻是否被静默（专属静默或全局静默）。
// 到期的静默规则即时清理；边界口径：now >= Until 即失效。
func (e *Engine) isSilenced(key string, now time.Time) bool {
	s := e.state
	muted := func(k string) bool {
		sil, ok := s.Silences[k]
		if !ok {
			return false
		}
		if !sil.Until.IsZero() && !now.Before(sil.Until) {
			delete(s.Silences, k)
			return false
		}
		return true
	}
	return muted(globalSilenceKey) || muted(key)
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
