package pipelineobs

import "sort"

// HealthSummary 返回按流水线、算子聚合的只读运行健康汇总。
//
// 唯一派生口径：所有字段只来自当前 StateFiring 的事件单——与逐条接入得到的
// 判定结果、ActiveAlerts() 完全同源。乱序、延迟到达、时钟回拨与重启恢复都
// 不会让汇总与活动告警各说各话（同一份持久化状态、同一把锁、无外部输入）。
//
// 算子全集 = 曾出现过统计（Buckets）或在途/历史任务（Tasks）的算子；
// 即便当前没有任何 firing 告警，该算子也会以健康形态出现在汇总中，
// 便于值班一眼确认“它在、且没病”。
func (e *Engine) HealthSummary() []PipelineHealth {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.state

	type opAgg struct {
		pipe, op     string
		stuck, stale bool
		count        int
		causes       map[Cause]bool
	}
	opMap := map[string]*opAgg{}
	get := func(pipe, op string) *opAgg {
		k := pipe + "|" + op
		a := opMap[k]
		if a == nil {
			a = &opAgg{pipe: pipe, op: op, causes: map[Cause]bool{}}
			opMap[k] = a
		}
		return a
	}

	// 算子全集：窗口统计 + 任务（与判定输入同源）。
	for _, b := range s.Buckets {
		get(b.Pipeline, b.Operator)
	}
	for _, t := range s.Tasks {
		get(t.Pipeline, t.Operator)
	}

	// 只统计当前 firing 的事件单，保证与 ActiveAlerts 条数完全对账。
	for _, inc := range s.Incidents {
		if inc.State != StateFiring {
			continue
		}
		a := get(inc.Pipeline, inc.Operator)
		a.count++
		a.causes[inc.Cause] = true
		switch inc.Cause {
		case CauseStuck:
			a.stuck = true
		case CauseStale:
			a.stale = true
		}
	}

	pipeMap := map[string]*PipelineHealth{}
	var pipeOrder []string
	for _, a := range opMap {
		ph := pipeMap[a.pipe]
		if ph == nil {
			ph = &PipelineHealth{Pipeline: a.pipe}
			pipeMap[a.pipe] = ph
			pipeOrder = append(pipeOrder, a.pipe)
		}
		causes := make([]Cause, 0, len(a.causes))
		for c := range a.causes {
			causes = append(causes, c)
		}
		sort.Slice(causes, func(i, j int) bool { return causes[i] < causes[j] })
		ph.Operators = append(ph.Operators, OperatorHealth{
			Pipeline: a.pipe, Operator: a.op,
			Stuck: a.stuck, Stale: a.stale,
			FiringCount: a.count, Causes: causes,
		})
		ph.FiringCount += a.count
		if a.stuck {
			ph.Stuck = true
		}
		if a.stale {
			ph.Stale = true
		}
	}

	sort.Strings(pipeOrder)
	out := make([]PipelineHealth, 0, len(pipeOrder))
	for _, name := range pipeOrder {
		ph := pipeMap[name]
		sort.Slice(ph.Operators, func(i, j int) bool { return ph.Operators[i].Operator < ph.Operators[j].Operator })
		out = append(out, *ph)
	}
	return out
}
