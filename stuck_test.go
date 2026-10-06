package pipelineobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// stuckCfg 在通用测试配置基础上打开卡死判定（阈值 60s）。
func stuckCfg() Config {
	c := testCfg()
	c.StuckThreshold = 60 * time.Second
	return c
}

func observe(t *testing.T, e *Engine) IngestResult {
	t.Helper()
	r, err := e.ObserveTime(context.Background())
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	for _, a := range r.Alerts {
		t.Logf("observe => ALERT cause=%-11s key=%s msg=%s", a.Cause, a.Key, a.Message)
	}
	for _, a := range r.Recovered {
		t.Logf("observe => RECOVERED cause=%-11s key=%s", a.Cause, a.Key)
	}
	return r
}

// TestStuckFiresOnWallClockOnly 核心场景：任务起了之后再无任何事件，
// 完全不接入新数据、只推进真实时钟，超过阈值也必须判出卡死。
// 边界口径：差值 == 阈值那一次即成立（>=）。
func TestStuckFiresOnWallClockOnly(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, stuckCfg(), clk, nil)

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	// 59s（< 60s 阈值）：不得判卡死。
	clk.Set(t0.Add(59 * time.Second))
	if r := observe(t, e); hasCause(r.Alerts, CauseStuck) {
		t.Fatalf("59s 不应判卡死，got %+v", r.Alerts)
	}

	// 恰好 60s：边界成立，判卡死。全程没有任何新事件。
	clk.Set(t0.Add(60 * time.Second))
	r := observe(t, e)
	if !hasCause(r.Alerts, CauseStuck) {
		t.Fatalf("恰好 60s 无进展应判卡死，got %+v", r.Alerts)
	}
	t.Logf("纯时钟推进判出卡死: %s", r.Alerts[0].Message)

	// 同一 firing 轮次继续推进：不刷屏。
	clk.Set(t0.Add(300 * time.Second))
	if r := observe(t, e); len(r.Alerts) != 0 {
		t.Fatalf("同一卡死轮次不应重复告警，got %+v", r.Alerts)
	}
}

// TestStuckDistinctFromStaleAndFailed 卡死与数据滞后、执行失败是
// 三个独立结论：同一算子上可同时成立但原因码各自区分。
func TestStuckDistinctFromStaleAndFailed(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, stuckCfg(), clk, nil)

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})

	// 30s：数据滞后（阈值 30s）成立，但卡死（60s）尚未成立。
	clk.Set(t0.Add(30 * time.Second))
	r := observe(t, e)
	if !hasCause(r.Alerts, CauseStale) {
		t.Fatal("30s 应判数据滞后")
	}
	if hasCause(r.Alerts, CauseStuck) {
		t.Fatal("30s 不应判卡死")
	}

	// 60s：卡死也成立，两者并存、原因码各自独立。
	clk.Set(t0.Add(60 * time.Second))
	r = observe(t, e)
	if !hasCause(r.Alerts, CauseStuck) {
		t.Fatal("60s 应判卡死")
	}
	keys := firingKeys(e)
	if !keys[alertKey("p", "agg", CauseStale)] || !keys[alertKey("p", "agg", CauseStuck)] {
		t.Fatalf("滞后与卡死应作为两条独立事件单并存，got %v", keys)
	}
	if hasCause(r.Alerts, CauseFailed) {
		t.Fatal("任务未失败，不应出现 failed 结论")
	}
	t.Log("滞后/卡死/失败三类结论独立：stale 与 stuck 并存，无 failed")
}

// TestStuckRecoversOnProgressAndFinish 任务重新有产出、或正常结束时，
// 卡死结论自动恢复；之后再次卡住能重新报出。
func TestStuckRecoversOnProgressAndFinish(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, stuckCfg(), clk, nil)

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
	clk.Set(t0.Add(60 * time.Second))
	if r := observe(t, e); !hasCause(r.Alerts, CauseStuck) {
		t.Fatal("前置：应已判卡死")
	}

	// 任务重新有产出（一条记录事件）-> 卡死恢复。
	clk.Set(t0.Add(61 * time.Second))
	r := ingest(t, e, Event{ID: "out1", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(61 * time.Second)})
	if !hasCause(r.Recovered, CauseStuck) {
		t.Fatalf("重新有产出后卡死应恢复，got recovered=%+v", r.Recovered)
	}
	t.Log("重新有产出 -> 卡死恢复")

	// 再次静默 60s：重新判卡死（新一轮，允许再次告警）。
	clk.Set(t0.Add(121*time.Second + 1))
	clk.Set(t0.Add(121 * time.Second))
	r = observe(t, e)
	if !hasCause(r.Alerts, CauseStuck) {
		t.Fatalf("恢复后再次卡住应能重新报出，got %+v", r.Alerts)
	}
	t.Log("恢复后再次卡住 -> 重新报出")

	// 任务正常结束 -> 卡死恢复，且之后不再因该任务报卡死。
	clk.Set(t0.Add(122 * time.Second))
	r = ingest(t, e, Event{ID: "fin", Pipeline: "p", Operator: "agg", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(122 * time.Second)})
	if !hasCause(r.Recovered, CauseStuck) {
		t.Fatalf("任务正常结束后卡死应恢复，got recovered=%+v", r.Recovered)
	}
	clk.Set(t0.Add(10 * time.Minute))
	if r := observe(t, e); hasCause(r.Alerts, CauseStuck) {
		t.Fatalf("任务已结束，不应再报卡死，got %+v", r.Alerts)
	}
	t.Log("任务结束 -> 卡死恢复且不再复发")
}

// TestStuckStableUnderClockRollback 时钟回拨时卡死结论不抖：
// 已判卡死不回撤、不重复告警；回拨期间也不会“误恢复”。
func TestStuckStableUnderClockRollback(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, stuckCfg(), clk, nil)

	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
	clk.Set(t0.Add(60 * time.Second))
	if r := observe(t, e); !hasCause(r.Alerts, CauseStuck) {
		t.Fatal("前置：应已判卡死")
	}

	// 时钟大幅回拨到任务开始之前：有效时间单调保持，结论必须稳定。
	clk.Set(t0.Add(-time.Hour))
	r := observe(t, e)
	if len(r.Alerts) != 0 || len(r.Recovered) != 0 {
		t.Fatalf("回拨不应产生新告警或误恢复，got alerts=%+v recovered=%+v", r.Alerts, r.Recovered)
	}
	if !firingKeys(e)[alertKey("p", "agg", CauseStuck)] {
		t.Fatal("回拨后卡死事件单必须保持 firing")
	}
	t.Log("时钟回拨：卡死结论保持，不抖、不误恢复")
}

// TestStuckSurvivesRestart 卡死状态跨重启延续：不丢、不重复刷屏；
// 重启后任务有产出则恢复。
func TestStuckSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	cfg := stuckCfg()

	// 第一个“进程”：任务卡死并告警。
	func() {
		clk := NewMockClock(t0)
		e := testEngine(t, cfg, clk, NewFileStore(path))
		ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
		clk.Set(t0.Add(60 * time.Second))
		if r := observe(t, e); !hasCause(r.Alerts, CauseStuck) {
			t.Fatal("前置：应已判卡死")
		}
		if err := e.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// 第二个“进程”：重启恢复，卡死事件单必须还在，且重复评估不刷屏。
	clk := NewMockClock(t0.Add(70 * time.Second))
	e2, err := Open(context.Background(), cfg, clk, NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	e2.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = e2.Close(context.Background()) })

	if !firingKeys(e2)[alertKey("p", "agg", CauseStuck)] {
		t.Fatal("重启后卡死事件单必须延续")
	}
	if r := observe(t, e2); len(r.Alerts) != 0 {
		t.Fatalf("重启后同一轮次不得重复告警，got %+v", r.Alerts)
	}

	// 重启后任务有产出 -> 卡死恢复。
	r := ingest(t, e2, Event{ID: "out1", Pipeline: "p", Operator: "agg", Kind: KindRecord, OccurredAt: t0.Add(70 * time.Second)})
	if !hasCause(r.Recovered, CauseStuck) {
		t.Fatalf("重启后有产出应恢复卡死，got recovered=%+v", r.Recovered)
	}
	t.Log("重启恢复：卡死延续、不刷屏，有产出后正常恢复")
}

// TestStuckDisabledWhenThresholdZero 兼容口径：StuckThreshold 为零值
// （旧配置）时卡死判定关闭，行为与旧版完全一致。
func TestStuckDisabledWhenThresholdZero(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil) // testCfg 未设置 StuckThreshold
	ingest(t, e, Event{ID: "start", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
	clk.Set(t0.Add(time.Hour))
	if r := observe(t, e); hasCause(r.Alerts, CauseStuck) {
		t.Fatalf("阈值为零应关闭卡死判定，got %+v", r.Alerts)
	}
	t.Log("StuckThreshold=0：卡死判定关闭（兼容旧配置）")
}
