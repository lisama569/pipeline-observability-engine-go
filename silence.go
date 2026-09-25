package pipelineobs

import (
	"context"
	"errors"
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
	e.state.Silences[key] = silenceState{Until: until, Note: note}
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
	e.state.Silences[key] = silenceState{Until: until, Note: note}
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
