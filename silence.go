package pipelineobs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var errClosed = errors.New("engine: closed")

// SilenceView 是一条静默规则的对外只读视图。
type SilenceView struct {
	ID       string
	Scope    SilenceScope
	Pipeline string
	Operator string
	Cause    Cause
	TaskID   string
	// Until 为零值表示永久静默。
	Until time.Time
	Note  string
}

func (r silenceRule) view() SilenceView {
	return SilenceView{
		ID: r.ID, Scope: r.Scope, Pipeline: r.Pipeline, Operator: r.Operator,
		Cause: r.Cause, TaskID: r.TaskID, Until: r.Until, Note: r.Note,
	}
}

// silenceRuleID 由范围字段生成确定性规则 ID：同范围重复添加幂等，
// 也使解除静默时能唯一定位到规则。
func silenceRuleID(scope SilenceScope, pipe, op string, cause Cause, taskID string) string {
	switch scope {
	case ScopeGlobal:
		return "global"
	case ScopePipeline:
		return "pipeline:" + pipe
	case ScopeOperator:
		return "operator:" + pipe + "|" + op
	case ScopeCause:
		if taskID != "" {
			return fmt.Sprintf("task:%s|%s|%s|%s", pipe, op, cause, taskID)
		}
		return "cause:" + pipe + "|" + op + "|" + string(cause)
	default:
		return "unknown"
	}
}

// newSilenceRule 按范围字段构造并校验一条静默规则。
// 范围口径唯一：宽范围字段必须留空，窄范围字段必须逐层给齐。
func newSilenceRule(now time.Time, scope SilenceScope, pipe, op string, cause Cause, taskID string, ttl time.Duration, note string) (silenceRule, error) {
	switch scope {
	case ScopeGlobal:
		pipe, op, cause, taskID = "", "", "", ""
	case ScopePipeline:
		if pipe == "" {
			return silenceRule{}, errors.New("silence: pipeline required for pipeline scope")
		}
		op, cause, taskID = "", "", ""
	case ScopeOperator:
		if pipe == "" || op == "" {
			return silenceRule{}, errors.New("silence: pipeline and operator required for operator scope")
		}
		cause, taskID = "", ""
	case ScopeCause:
		if pipe == "" || op == "" || cause == "" {
			return silenceRule{}, errors.New("silence: pipeline, operator and cause required for cause scope")
		}
		if taskID != "" && cause != CauseFailed {
			return silenceRule{}, errors.New("silence: task ID only applies to task_failed scope")
		}
	default:
		return silenceRule{}, fmt.Errorf("silence: unknown scope %q", scope)
	}
	var until time.Time
	if ttl > 0 {
		until = now.Add(ttl)
	}
	r := silenceRule{
		Scope: scope, Pipeline: pipe, Operator: op, Cause: cause, TaskID: taskID,
		Until: until, Note: note,
	}
	r.ID = silenceRuleID(scope, pipe, op, cause, taskID)
	return r, nil
}

// covers 判断规则是否覆盖某个事件单（事件单由 流水线/算子/原因/告警键描述）。
// 宽范围规则覆盖其名下全部更细问题；单次任务规则要求告警键精确一致。
func (r silenceRule) covers(pipe, op string, cause Cause, incidentKey string) bool {
	switch r.Scope {
	case ScopeGlobal:
		return true
	case ScopePipeline:
		return r.Pipeline == pipe
	case ScopeOperator:
		return r.Pipeline == pipe && r.Operator == op
	case ScopeCause:
		if r.Pipeline != pipe || r.Operator != op || r.Cause != cause {
			return false
		}
		if r.TaskID == "" {
			return true // 算子名下该原因的全部实例（如所有任务失败/卡住）
		}
		return incidentKey == failureAlertKey(r.Pipeline, r.Operator, r.TaskID)
	default:
		return false
	}
}

// addRuleLocked 写入/覆盖一条静默规则并持久化。
func (e *Engine) addRuleLocked(r silenceRule) error {
	e.state.SilenceRules[r.ID] = r
	e.logf("silence added id=%s scope=%s until=%s note=%q",
		r.ID, r.Scope, r.Until.Format(time.RFC3339), r.Note)
	return e.persistLocked(context.Background())
}

// SilencePipeline 静默整条流水线（其名下所有算子、所有原因）。
func (e *Engine) SilencePipeline(pipeline string, ttl time.Duration, note string) error {
	return e.addSilence(ScopePipeline, pipeline, "", "", "", ttl, note)
}

// SilenceOperator 静默某个算子名下一整类/全部问题（所有原因，
// 包括该算子名下所有任务失败、所有任务卡住，含已在报与静默期内新冒出的）。
func (e *Engine) SilenceOperator(pipeline, operator string, ttl time.Duration, note string) error {
	return e.addSilence(ScopeOperator, pipeline, operator, "", "", ttl, note)
}

// Silence 添加静默规则，参数编码范围（口径唯一）：
//   - pipeline/operator/cause 全空：全局静默；
//   - 仅 pipeline：整条流水线静默；
//   - pipeline+operator（cause 为空）：单个算子静默；
//   - pipeline+operator+cause：单算子单原因静默。
//
// ttl=0 表示永久静默，否则从当前有效时间起持续 ttl；
// 到期边界：观察时刻 now >= Until 时规则立即失效。
// 静默只压对外通知，内部状态机照常推进并持久化。
func (e *Engine) Silence(pipeline, operator string, cause Cause, ttl time.Duration, note string) error {
	scope := ScopeGlobal
	switch {
	case pipeline != "" && operator != "" && cause != "":
		scope = ScopeCause
	case pipeline != "" && operator != "":
		scope = ScopeOperator
	case pipeline != "":
		scope = ScopePipeline
	}
	return e.addSilence(scope, pipeline, operator, cause, "", ttl, note)
}

// SilenceFailure 精确静默某次任务失败告警（键含 TaskID）。
func (e *Engine) SilenceFailure(pipeline, operator, taskID string, ttl time.Duration, note string) error {
	if taskID == "" {
		return errors.New("silence: taskID required")
	}
	return e.addSilence(ScopeCause, pipeline, operator, CauseFailed, taskID, ttl, note)
}

func (e *Engine) addSilence(scope SilenceScope, pipe, op string, cause Cause, taskID string, ttl time.Duration, note string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errClosed
	}
	r, err := newSilenceRule(e.state.EffectiveNow, scope, pipe, op, cause, taskID, ttl, note)
	if err != nil {
		return err
	}
	return e.addRuleLocked(r)
}

// RemoveSilence 解除按 (pipeline, operator, cause) 定位的静默规则，
// 编码方式与 Silence 相同（找不到也不报错）。解除本身不立即补发，
// 下一次评估（ObserveTime/接入）时若问题仍在会重新发出一条告警。
func (e *Engine) RemoveSilence(pipeline, operator string, cause Cause) error {
	scope := ScopeGlobal
	switch {
	case pipeline != "" && operator != "" && cause != "":
		scope = ScopeCause
	case pipeline != "" && operator != "":
		scope = ScopeOperator
	case pipeline != "":
		scope = ScopePipeline
	}
	return e.removeRuleByID(silenceRuleID(scope, pipeline, operator, cause, ""))
}

// RemoveSilenceFailure 解除某次任务失败的精确静默。
func (e *Engine) RemoveSilenceFailure(pipeline, operator, taskID string) error {
	return e.removeRuleByID(silenceRuleID(ScopeCause, pipeline, operator, CauseFailed, taskID))
}

// RemoveSilencePipeline 解除整条流水线静默。
func (e *Engine) RemoveSilencePipeline(pipeline string) error {
	return e.removeRuleByID(silenceRuleID(ScopePipeline, pipeline, "", "", ""))
}

// RemoveSilenceOperator 解除单个算子静默。
func (e *Engine) RemoveSilenceOperator(pipeline, operator string) error {
	return e.removeRuleByID(silenceRuleID(ScopeOperator, pipeline, operator, "", ""))
}

// RemoveSilenceRule 按规则 ID 解除静默（ListSilences 可查到 ID）。
func (e *Engine) RemoveSilenceRule(ruleID string) error { return e.removeRuleByID(ruleID) }

func (e *Engine) removeRuleByID(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errClosed
	}
	if _, ok := e.state.SilenceRules[id]; ok {
		delete(e.state.SilenceRules, id)
		e.logf("silence removed id=%s", id)
		return e.persistLocked(context.Background())
	}
	return nil
}

// ListSilences 返回当前全部静默规则（含尚未到期/永久），按 ID 排序。
func (e *Engine) ListSilences() []SilenceView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SilenceView, 0, len(e.state.SilenceRules))
	for _, r := range e.state.SilenceRules {
		out = append(out, r.view())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// sweepExpiredSilencesLocked 惰性清理到期规则：边界 now >= Until 即失效。
// 在每次评估开始时统一执行一次，使“到期后重新报警”在同一次评估内生效。
func (e *Engine) sweepExpiredSilencesLocked(now time.Time) {
	for id, r := range e.state.SilenceRules {
		if !r.Until.IsZero() && !now.Before(r.Until) {
			delete(e.state.SilenceRules, id)
			e.logf("silence expired id=%s at=%s", id, now.Format(time.RFC3339))
		}
	}
}

// isMutedLocked 判断某事件单在 now 时刻是否被任一未到期静默规则覆盖。
// 调用前应已 sweepExpiredSilencesLocked。命中即压通知，与规则层级无关；
// 多种范围同时命中时不影响结论（都只压通知），故优先级口径唯一。
func (e *Engine) isMutedLocked(pipe, op string, cause Cause, incidentKey string) bool {
	for _, r := range e.state.SilenceRules {
		if r.covers(pipe, op, cause, incidentKey) {
			return true
		}
	}
	return false
}

// describeSilenceCoverage 仅供日志：列出命中某事件单的规则 ID（排序）。
func (e *Engine) describeSilenceCoverage(pipe, op string, cause Cause, key string) string {
	var ids []string
	for _, r := range e.state.SilenceRules {
		if r.covers(pipe, op, cause, key) {
			ids = append(ids, r.ID)
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}
