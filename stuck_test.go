package pipelineobs

import (
	"context"
	"testing"
	"time"
)

// TestTaskStuckDetectedWithoutEvents 核心新增行为：任务起后完全没有任何
// 后续事件、也没有结束，仅靠真实时钟（ObserveTime）往前走，超过阈值也必须
// 明确判出“卡住”；阈值前一刻不报，恰好到阈值当次必报（>= 边界唯一）。
func TestTaskStuckDetectedWithoutEvents(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil) // TaskStuckThreshold=45s

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	// 同算子没有任何新数据。推进到 44s（< 45s）：不得判卡住。
	clk.Set(t0.Add(44 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("44s 不应判卡住，got %+v", res.Alerts)
	}
	t.Log("输入：仅 task_started@00s，之后 0 条事件，观察时刻推进到 00:44 => 判定：未卡住（44s < 45s）")

	// 恰好 45s：边界成立，当次必报。
	clk.Set(t0.Add(45 * time.Second))
	res, _ = e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("恰好 45s（== 阈值）应判卡住，got %+v", res.Alerts)
	}
	if hasCause(res.Alerts, CauseTimeout) {
		t.Fatalf("水位从未推进，不应误判为任务超时（卡住与超时必须区分），got %+v", res.Alerts)
	}
	if hasCause(res.Alerts, CauseFailed) {
		t.Fatalf("任务没有失败事件，不应误判执行失败，got %+v", res.Alerts)
	}
	t.Logf("输入：观察时刻推进到 00:45（无任何事件）=> 判定依据：有效现在-最近进展 45s ≥ 45s => %s",
		res.Alerts[0].Message)

	// 同一轮持续成立、时间继续走：只此一条，不刷屏。
	clk.Set(t0.Add(10 * time.Minute))
	res, _ = e.ObserveTime(context.Background())
	if len(res.Alerts) != 0 {
		t.Fatalf("同一卡住轮次持续期间不得重复告警，got %+v", res.Alerts)
	}
}

// TestTaskStuckDistinctFromStaleAndTimeout 卡住与数据滞后、任务超时
// 三种结论原因码不同、可同时成立、互不吞掉。
func TestTaskStuckDistinctFromStaleAndTimeout(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	// 阈值：stale 30s、stuck 45s、timeout 90s（事件时间水位）。

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	// 30s：只有数据滞后（任务还没到卡住阈值）。
	clk.Set(t0.Add(30 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStale) || hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("30s 应只判数据滞后，got %+v", res.Alerts)
	}

	// 45s：卡住加入（此时滞后已在同一轮，不重复发）。两者并存、原因码不同。
	clk.Set(t0.Add(45 * time.Second))
	res, _ = e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("45s 应判卡住，got %+v", res.Alerts)
	}
	if !firingKeys(e)[alertKey("p", "agg", CauseStale)] {
		t.Fatal("数据滞后应仍 firing，与卡住并存")
	}
	t.Log("数据滞后与任务卡住同时 firing，原因码 data_stale / task_stuck 各自独立")

	// 来一条迟到的高事件时间记录：它属于“该算子的一次进展/接入”，
	// 因此按口径数据滞后恢复（有新接入），但事件时间水位越过 90s 又补判超时；
	// 卡住不依赖水位、也不要求“没有接入”，仍保持 firing。
	r := ingest(t, e, Event{ID: "late", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(95 * time.Second)})
	if !hasCause(r.Alerts, CauseTimeout) {
		t.Fatalf("事件时间水位 95s 应另判超时，got %+v", r.Alerts)
	}
	if hasCause(r.Alerts, CauseStuck) {
		t.Fatalf("新记录是进展，卡住应恢复而非继续，got %+v", r.Alerts)
	}
	keys := firingKeys(e)
	if !keys[alertKey("p", "agg", CauseTimeout)] {
		t.Fatalf("超时应 firing，实际 %v", keys)
	}
	for _, k := range []string{
		alertKey("p", "agg", CauseStuck),
		alertKey("p", "agg", CauseStale),
	} {
		if keys[k] {
			t.Fatalf("新记录接入后 %s 应已恢复，实际仍 firing %v", k, keys)
		}
	}
	t.Log("进展事件后：滞后/卡住恢复、超时独立成立；三类原因判定口径互不混淆")
}

// TestTaskStuckRecoversOnProgressAndCanRearm 任务重新有产出后卡住自动恢复；
// 之后再次无进展卡住必须能重新报出（FireCount 递增）；正常结束也恢复。
func TestTaskStuckRecoversOnProgressAndCanRearm(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	clk.Set(t0.Add(45 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatal("前置：应判卡住")
	}
	key := alertKey("p", "agg", CauseStuck)
	cnt := 0
	for _, a := range res.Alerts {
		if a.Key == key {
			cnt = a.FireCount
		}
	}

	// 重新有产出（一条记录）：进展刷新，卡住恢复。
	clk.Set(t0.Add(50 * time.Second))
	r := ingest(t, e, Event{ID: "rec1", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(50 * time.Second)})
	var recoveredStuck bool
	for _, a := range r.Recovered {
		if a.Cause == CauseStuck {
			recoveredStuck = true
		}
	}
	if !recoveredStuck {
		t.Fatalf("任务重新有产出后卡住应自动恢复，got recovered=%+v", r.Recovered)
	}
	t.Log("卡住任务重新有数据产出 => task_stuck 自动恢复")

	// 之后再无进展 45s：必须能重新报出，FireCount+1。
	clk.Set(t0.Add(95 * time.Second))
	res, _ = e.ObserveTime(context.Background())
	var again Alert
	for _, a := range res.Alerts {
		if a.Key == key {
			again = a
		}
	}
	if again.Cause != CauseStuck {
		t.Fatalf("恢复后再次卡住必须重新报出，got %+v", res.Alerts)
	}
	if again.FireCount != cnt+1 {
		t.Fatalf("重新卡住 FireCount 应为 %d，got %d", cnt+1, again.FireCount)
	}
	t.Logf("再次无进展超过阈值 => 重新报卡住，FireCount=%d", again.FireCount)

	// 任务正常结束：卡住恢复（不是失败）。
	r = ingest(t, e, Event{ID: "finish", Pipeline: "p", Operator: "agg", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(95 * time.Second)})
	recoveredStuck = false
	for _, a := range r.Recovered {
		if a.Cause == CauseStuck {
			recoveredStuck = true
		}
		if a.Cause == CauseFailed {
			t.Fatal("正常结束不应产生失败结论")
		}
	}
	if !recoveredStuck {
		t.Fatalf("任务正常结束后卡住应恢复，got %+v", r.Recovered)
	}
}

// TestTaskStuckClockRollbackStable 时钟回拨时卡住结论不抖、不误恢复、
// 不重复告警；恢复前进后口径照旧。
func TestTaskStuckClockRollbackStable(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	clk.Set(t0.Add(60 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatal("60s 应已判卡住")
	}

	// 时钟大幅回拨到起点：有效时间被单调保护钉在 00:60，卡住依旧成立，
	// 但属于同一 firing 轮次：无新告警、无恢复。
	clk.Set(t0)
	res, _ = e.ObserveTime(context.Background())
	if len(res.Alerts) != 0 {
		t.Fatalf("回拨不应产生新告警，got %+v", res.Alerts)
	}
	if len(res.Recovered) != 0 {
		t.Fatalf("回拨不应导致卡住误恢复，got %+v", res.Recovered)
	}
	if len(e.ActiveAlerts()) == 0 {
		t.Fatal("回拨后卡住事件单必须保留")
	}
	t.Log("时钟回拨 60s：卡住结论保持 firing，不恢复、不重复")

	// 时钟再往前走：同一轮仍不重复。
	clk.Set(t0.Add(120 * time.Second))
	res, _ = e.ObserveTime(context.Background())
	if len(res.Alerts) != 0 {
		t.Fatalf("同一卡住轮次恢复前进后仍不应重复告警，got %+v", res.Alerts)
	}
}
