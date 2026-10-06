package pipelineobs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

var errClosed = errors.New("engine: closed")

// Silence 添加一条静默规则并持久化（重启后延续）。
//   - pipeline/operator/cause 全部给空串：全局静默；
//   - 否则静默对应的告警根因键；
//   - ttl 为 0 表示永久静默，否则从当前有效时间起持续 ttl；
//   - 静默期内触发的告警照常推进状态机，只是不对外发出（解除静默后
//     不会补发——同一 firing 轮次只通知一条，这是唯一口径）；
//   - 静默到期边界：观察时刻 now >= Until 时规则立即失效。
func (e *Engine) Silence(pipeline, operator string, cause Cause, ttl time.Duration, note string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := globalSilenceKey
	if pipeline != "" || operator != "" || cause != "" {
		key = alertKey(pipeline, operator, cause)
	}
	var until time.Time
	if ttl > 0 {
		until = e.state.EffectiveNow.Add(ttl)
	}
	sil := silenceState{Until: until, Note: note}
	if key != globalSilenceKey {
		sil.Key = key // 逐根因精确匹配（保持旧版语义）
	}
	e.state.Silences[key] = sil
	e.logf("silence added key=%s until=%s note=%q", key, until.Format(time.RFC3339), note)
	return e.persistLocked(context.Background())
}

// SilenceFailure 精确静默某次任务失败告警（键含 TaskID）。
func (e *Engine) SilenceFailure(pipeline, operator, taskID string, ttl time.Duration, note string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var until time.Time
	if ttl > 0 {
		until = e.state.EffectiveNow.Add(ttl)
	}
	key := failureAlertKey(pipeline, operator, taskID)
	e.state.Silences[key] = silenceState{Until: until, Note: note, Key: key}
	e.logf("silence added key=%s until=%s note=%q", key, until.Format(time.RFC3339), note)
	return e.persistLocked(context.Background())
}

// RemoveSilence 移除一条静默规则并持久化（找不到也不报错）。
func (e *Engine) RemoveSilence(pipeline, operator string, cause Cause) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := globalSilenceKey
	if pipeline != "" || operator != "" || cause != "" {
		key = alertKey(pipeline, operator, cause)
	}
	delete(e.state.Silences, key)
	return e.persistLocked(context.Background())
}

// SilenceScope 描述一条按范围生效的静默规则。空字段为通配：
//   - 仅 Pipeline：静默整条流水线的全部告警；
//   - Pipeline+Operator：静默该算子名下全部告警（含各类 cause 与
//     各任务实例的失败），已触发的与静默期内新出现的都覆盖；
//   - 再叠加 Cause：只静默该范围内某一类问题；
//   - 全部为空：等价于全局静默。
type SilenceScope struct {
	Pipeline string
	Operator string
	Cause    Cause
	TaskID   string
}

// SilenceRuleView 是一条静默规则的只读视图。
type SilenceRuleView struct {
	ID    string
	Scope SilenceScope
	// ExactKey 非空表示这是逐根因精确静默（旧版 Silence 语义）。
	ExactKey  string
	Until     time.Time
	Note      string
	Permanent bool
}

// SilenceScoped 添加一条按范围生效的静默规则并持久化，返回规则 ID。
// ttl 为 0 表示永久静默；相同范围的重复调用会覆盖旧规则（幂等）。
func (e *Engine) SilenceScoped(scope SilenceScope, ttl time.Duration, note string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := scopedSilenceID(scope)
	var until time.Time
	if ttl > 0 {
		until = e.state.EffectiveNow.Add(ttl)
	}
	e.state.Silences[id] = silenceState{
		Until: until, Note: note,
		Pipeline: scope.Pipeline, Operator: scope.Operator,
		Cause: scope.Cause, TaskID: scope.TaskID,
	}
	e.logf("scoped silence added id=%s scope=%+v until=%s note=%q",
		id, scope, until.Format(time.RFC3339), note)
	return id, e.persistLocked(context.Background())
}

// RemoveSilenceRule 按规则 ID 移除静默（找不到也不报错）。
// 移除后若被覆盖的问题仍在 firing，下一次评估会补发一条告警。
func (e *Engine) RemoveSilenceRule(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.state.Silences, id)
	e.logf("silence removed id=%s", id)
	return e.persistLocked(context.Background())
}

// ListSilences 返回当前全部（未到期）静默规则的只读视图，按 ID 排序。
func (e *Engine) ListSilences() []SilenceRuleView {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.state.EffectiveNow
	out := make([]SilenceRuleView, 0, len(e.state.Silences))
	for id, sil := range e.state.Silences {
		if !sil.Until.IsZero() && !now.Before(sil.Until) {
			continue // 已到期（等待惰性清理），不展示
		}
		v := SilenceRuleView{
			ID: id, Until: sil.Until, Note: sil.Note,
			Permanent: sil.Until.IsZero(),
		}
		if sil.Key != "" {
			v.ExactKey = sil.Key
		} else {
			v.Scope = SilenceScope{
				Pipeline: sil.Pipeline, Operator: sil.Operator,
				Cause: sil.Cause, TaskID: sil.TaskID,
			}
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// scopedSilenceID 由范围生成确定性的规则 ID（相同范围 -> 相同 ID）。
// 全部字段为空时归一到全局静默键，与 Silence("","","") 等价。
func scopedSilenceID(scope SilenceScope) string {
	if scope.Pipeline == "" && scope.Operator == "" && scope.Cause == "" && scope.TaskID == "" {
		return globalSilenceKey
	}
	return fmt.Sprintf("scope|%s|%s|%s|%s", scope.Pipeline, scope.Operator, scope.Cause, scope.TaskID)
}
