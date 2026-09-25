package pipelineobs

import (
	"context"
	"sort"
	"time"
)

// ObserveTime 不接入事件，仅推进“观察时刻”并重新评估时效类告警。
//
// 用于没有新事件到达时的数据滞后/任务超时检测（例如周期调度器调用）。
// now 取自引擎时钟；时钟回拨时仍受 EffectiveNow 单调保护。
func (e *Engine) ObserveTime(ctx context.Context) (IngestResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return IngestResult{}, errClosed
	}
	now := e.advanceNowLocked()

	// 对所有出现过的算子评估数据滞后；超时/失败在 evaluateAlerts 内统一扫描。
	touched := map[string]bool{}
	for k := range e.state.LastIngest {
		touched[k] = true
	}
	fired, recovered := e.evaluateAlerts(now, touched)
	res := IngestResult{Alerts: fired, Recovered: recovered}
	if err := e.persistLocked(ctx); err != nil {
		return res, err
	}
	return res, nil
}

// advanceNowLocked 取时钟当前值并施加单调保护，返回有效时间。
func (e *Engine) advanceNowLocked() time.Time {
	rawNow := e.clock.Now().UTC()
	now := rawNow
	if now.Before(e.state.EffectiveNow) {
		e.logf("clock rollback detected: raw=%s effective=%s -> hold",
			rawNow.Format(time.RFC3339Nano), e.state.EffectiveNow.Format(time.RFC3339Nano))
		now = e.state.EffectiveNow
	} else {
		e.state.EffectiveNow = now
	}
	return now
}

// BucketView 是对外的窗口聚合只读视图。
type BucketView struct {
	Pipeline, Operator     string
	WindowStart, WindowEnd time.Time
	Closed                 bool

	RecordCount  int64
	ByteCount    int64
	LatencyAvgMS float64

	TaskStarted, TaskFinished, TaskFailed int64

	MinEventTime, MaxEventTime time.Time
}

// Buckets 返回当前全部聚合窗口的快照，按 (流水线,算子,起点) 排序。
func (e *Engine) Buckets() []BucketView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]BucketView, 0, len(e.state.Buckets))
	for _, b := range e.state.Buckets {
		v := BucketView{
			Pipeline: b.Pipeline, Operator: b.Operator,
			WindowStart: b.WindowStart, WindowEnd: b.WindowEnd, Closed: b.Closed,
			RecordCount: b.RecordCount, ByteCount: b.ByteCount,
			TaskStarted: b.TaskStarted, TaskFinished: b.TaskFinished, TaskFailed: b.TaskFailed,
			MinEventTime: b.MinEventTime, MaxEventTime: b.MaxEventTime,
		}
		if b.RecordCount > 0 {
			v.LatencyAvgMS = float64(b.LatencySumMS) / float64(b.RecordCount)
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pipeline != out[j].Pipeline {
			return out[i].Pipeline < out[j].Pipeline
		}
		if out[i].Operator != out[j].Operator {
			return out[i].Operator < out[j].Operator
		}
		return out[i].WindowStart.Before(out[j].WindowStart)
	})
	return out
}

// ActiveAlerts 返回当前仍处于 firing 状态的告警视图。
func (e *Engine) ActiveAlerts() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Alert
	for _, inc := range e.state.Incidents {
		if inc.State == StateFiring {
			out = append(out, inc.toAlert(""))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
