package pipelineobs

import (
	"context"
	"testing"
	"time"
)

func hasCause(alerts []Alert, cause Cause) bool {
	for _, a := range alerts {
		if a.Cause == cause {
			return true
		}
	}
	return false
}

func firingKeys(e *Engine) map[string]bool {
	m := map[string]bool{}
	for _, a := range e.ActiveAlerts() {
		m[a.Key] = true
	}
	return m
}

// TestStalenessBoundary 数据新鲜度边界口径：差值 < 阈值不告警，
// 差值 == 阈值立即告警（>= 口径唯一）。
func TestStalenessBoundary(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	r := ingest(t, e, Event{ID: "s1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	if len(r.Alerts) != 0 {
		t.Fatalf("首次接入不应有滞后告警，got %d", len(r.Alerts))
	}

	// 推进 29s（< 30s 阈值）：不告警。
	clk.Set(t0.Add(29 * time.Second))
	res, err := e.ObserveTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Alerts) != 0 {
		t.Fatalf("29s 不应判滞后，got %+v", res.Alerts)
	}

	// 推进到恰好 30s：边界成立，告警。
	clk.Set(t0.Add(30 * time.Second))
	res, _ = e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStale) {
		t.Fatalf("恰好 30s 应判数据滞后，got %+v", res.Alerts)
	}
	t.Logf("边界 30s 触发滞后告警: %s", res.Alerts[0].Message)

	// 再来一条新事件（新鲜度恢复）。
	r2 := ingest(t, e, Event{ID: "s2", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(30 * time.Second)})
	if len(r2.Recovered) != 1 || r2.Recovered[0].Cause != CauseStale {
		t.Fatalf("新数据到达后滞后告警应恢复，got recovered=%+v", r2.Recovered)
	}
}

// TestTaskTimeoutVsFailureReasons 区分超时与执行失败两种原因，
// 且超时随任务结束而恢复。
func TestTaskTimeoutVsFailureReasons(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	// 水位推进到 t0+90s：恰好达到超时阈值（>= 边界）。
	clk.Set(t0.Add(90 * time.Second))
	r := ingest(t, e, Event{ID: "tick90", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(90 * time.Second)})
	if !hasCause(r.Alerts, CauseTimeout) {
		t.Fatalf("运行跨度恰好 90s 应判超时，got %+v", r.Alerts)
	}
	if hasCause(r.Alerts, CauseFailed) {
		t.Fatalf("任务尚未失败，不应有 failed 告警")
	}

	// 条件仍成立，再推进：同一 firing 轮次不得重复产生告警（抑制）。
	clk.Set(t0.Add(200 * time.Second))
	r = ingest(t, e, Event{ID: "tick200", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(200 * time.Second)})
	if len(r.Alerts) != 0 {
		t.Fatalf("同一超时根因在抑制窗口内只应产生一条告警，重复得到 %d 条", len(r.Alerts))
	}

	// 任务正常结束 -> 超时告警恢复（不是失败）。
	clk.Set(t0.Add(201 * time.Second))
	r = ingest(t, e, Event{ID: "finish", Pipeline: "p", Operator: "agg", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(200 * time.Second)})
	if len(r.Recovered) != 1 || r.Recovered[0].Cause != CauseTimeout {
		t.Fatalf("任务结束后超时应恢复，got %+v", r.Recovered)
	}

	// 另一个任务失败：失败原因单独触发。
	ingest(t, e, Event{ID: "start2", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T2", OccurredAt: t0.Add(201 * time.Second)})
	r = ingest(t, e, Event{ID: "fail2", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T2", OccurredAt: t0.Add(205 * time.Second), Failed: true, ErrorMessage: "boom"})
	if !hasCause(r.Alerts, CauseFailed) {
		t.Fatalf("失败任务应产生 task_failed 告警，got %+v", r.Alerts)
	}
	t.Logf("失败原因告警: %s", r.Alerts[0].Message)
}

// TestReWorsenAfterRecovery 恢复后再次恶化必须重新触发，FireCount 递增。
func TestReWorsenAfterRecovery(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})

	clk.Set(t0.Add(30 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStale) {
		t.Fatal("首次恶化应触发")
	}
	key := res.Alerts[0].Key
	cnt1 := res.Alerts[0].FireCount

	// 恢复。
	ingest(t, e, Event{ID: "x2", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(30 * time.Second)})
	// 再次恶化。
	clk.Set(t0.Add(70 * time.Second))
	res, _ = e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStale) {
		t.Fatal("恢复后再次恶化应重新触发")
	}
	var a Alert
	for _, x := range res.Alerts {
		if x.Key == key {
			a = x
		}
	}
	if a.FireCount != cnt1+1 {
		t.Fatalf("FireCount 应从 %d 增至 %d，got %d", cnt1, cnt1+1, a.FireCount)
	}
	t.Logf("恢复后重新触发成功，FireCount=%d", a.FireCount)
}

// TestClockRollbackStability 时钟回拨期间结论稳定：不重复告警、
// 已触发的不消失、新鲜度不被错误恢复。
func TestClockRollbackStability(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})

	// 触发滞后。
	clk.Set(t0.Add(40 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStale) {
		t.Fatal("应已滞后")
	}

	// 时钟大幅回拨到 t0：有效时间保持在 10:00:40，
	// 滞后条件依旧成立，但属于同一 firing 轮次 -> 无新告警、无恢复。
	clk.Set(t0)
	res, _ = e.ObserveTime(context.Background())
	if len(res.Alerts) != 0 {
		t.Fatalf("回拨不应产生新告警，got %+v", res.Alerts)
	}
	if len(res.Recovered) != 0 {
		t.Fatalf("回拨不应造成恢复，got %+v", res.Recovered)
	}
	if len(e.ActiveAlerts()) != 1 {
		t.Fatalf("已触发告警必须保留，got %d", len(e.ActiveAlerts()))
	}
	t.Log("时钟回拨后：告警保留且不重复，结论稳定")
}

// TestTimeoutIsEventTimeDriven 超时由事件时间水位驱动：
// 即便处理时间不推进，只要迟到事件把水位推过阈值，结论一致可复现。
func TestTimeoutIsEventTimeDriven(t *testing.T) {
	clk := NewMockClock(t0.Add(10 * time.Second)) // 处理时间几乎不动
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "s", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T", OccurredAt: t0})

	// 迟到事件：事件时间 t0+90s，处理时间仍停留在 t0+10s 附近。
	clk.Set(t0.Add(10 * time.Second))
	r := ingest(t, e, Event{ID: "late-tick", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(90 * time.Second)})
	if !hasCause(r.Alerts, CauseTimeout) {
		t.Fatalf("事件时间水位达到 90s 即应判超时（与处理时钟无关），got %+v", r.Alerts)
	}
}

// TestOutOfOrderTaskLifecycle finished 先于 started 到达（started 迟到）时，
// 任务仍被正确判定为已失败，且迟到的 started 不会把它重置为运行中。
func TestOutOfOrderTaskLifecycle(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	// 先到 finished（引擎补建任务并记失败）。
	r := ingest(t, e, Event{ID: "f-late", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "TX", OccurredAt: t0.Add(10 * time.Second), Failed: true, ErrorMessage: "late boom"})
	if !hasCause(r.Alerts, CauseFailed) {
		t.Fatalf("未见 started 的失败也必须被捕获，got %+v", r.Alerts)
	}

	// 迟到的 started 到达：不得覆盖、不得重置、不得再告警。
	r2 := ingest(t, e, Event{ID: "s-late", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "TX", OccurredAt: t0})
	if hasCause(r2.Alerts, CauseFailed) {
		t.Fatalf("迟到的 started 不应重复产生失败告警")
	}
	if hasCause(r2.Alerts, CauseTimeout) {
		t.Fatalf("任务已结束，迟到的 started 不应让它重新变运行中而触发超时")
	}
	for _, a := range e.ActiveAlerts() {
		if a.Cause == CauseTimeout {
			t.Fatalf("不应存在超时告警：%+v", a)
		}
	}
	t.Log("乱序任务生命周期结论稳定：失败不丢、不被迟到 started 重置")
}
