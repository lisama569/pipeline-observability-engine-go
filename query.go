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
	e.logAlertsLocked(now, fired, recovered)
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

// OperatorHealth 是单个算子的运行健康只读视图。
type OperatorHealth struct {
	Pipeline, Operator string
	// Stuck 当前是否有卡死（task_stuck）告警在 firing。
	Stuck bool
	// Stale 当前是否有数据滞后（data_stale）告警在 firing。
	Stale bool
	// RunningTasks 当前仍在运行的任务数。
	RunningTasks int
	// ActiveAlerts 当前 firing 的告警总数（含超时/失败等全部原因）。
	ActiveAlerts int
	// Causes 当前 firing 告警的原因集合（升序、去重）。
	Causes []Cause
}

// PipelineHealth 是单条流水线的健康汇总（由其算子聚合）。
type PipelineHealth struct {
	Pipeline string
	// Stuck / Stale：任一算子成立即成立。
	Stuck, Stale bool
	ActiveAlerts int
	Operators    []OperatorHealth
}

// Health 返回按流水线、算子聚合的运行健康汇总（只读）。
//
// 一致性口径：汇总完全由已持久化的事件单（firing 状态）与任务状态
// 推导，与 ActiveAlerts、逐条接入的判定结果严格一致；不推进时间、
// 不修改任何状态。需要刷新时效结论时先调用 ObserveTime。
func (e *Engine) Health() []PipelineHealth {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 收集出现过的全部 (pipeline, operator)：接入过、有任务、或有事件单的。
	type opRef struct{ pipe, op string }
	seen := map[opRef]bool{}
	mark := func(pipe, op string) { seen[opRef{pipe, op}] = true }
	for k := range e.state.LastIngest {
		p, o := splitOperatorKey(k)
		mark(p, o)
	}
	for _, t := range e.state.Tasks {
		mark(t.Pipeline, t.Operator)
	}
	for _, inc := range e.state.Incidents {
		mark(inc.Pipeline, inc.Operator)
	}

	ops := make(map[opRef]*OperatorHealth, len(seen))
	for ref := range seen {
		ops[ref] = &OperatorHealth{Pipeline: ref.pipe, Operator: ref.op}
	}

	// 在途任务数。
	for _, t := range e.state.Tasks {
		if t.Running {
			ops[opRef{t.Pipeline, t.Operator}].RunningTasks++
		}
	}

	// firing 事件单 -> 算子健康（与 ActiveAlerts 同一数据源）。
	for _, inc := range e.state.Incidents {
		if inc.State != StateFiring {
			continue
		}
		h := ops[opRef{inc.Pipeline, inc.Operator}]
		h.ActiveAlerts++
		switch inc.Cause {
		case CauseStuck:
			h.Stuck = true
		case CauseStale:
			h.Stale = true
		}
		h.Causes = append(h.Causes, inc.Cause)
	}

	// 汇总到流水线。
	byPipe := map[string]*PipelineHealth{}
	for ref, h := range ops {
		sort.Slice(h.Causes, func(i, j int) bool { return h.Causes[i] < h.Causes[j] })
		p := byPipe[ref.pipe]
		if p == nil {
			p = &PipelineHealth{Pipeline: ref.pipe}
			byPipe[ref.pipe] = p
		}
		p.Stuck = p.Stuck || h.Stuck
		p.Stale = p.Stale || h.Stale
		p.ActiveAlerts += h.ActiveAlerts
		p.Operators = append(p.Operators, *h)
	}
	out := make([]PipelineHealth, 0, len(byPipe))
	for _, p := range byPipe {
		sort.Slice(p.Operators, func(i, j int) bool { return p.Operators[i].Operator < p.Operators[j].Operator })
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pipeline < out[j].Pipeline })
	return out
}
