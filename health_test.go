package pipelineobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func findOp(t *testing.T, hs []PipelineHealth, pipe, op string) OperatorHealth {
	t.Helper()
	for _, p := range hs {
		for _, o := range p.Operators {
			if p.Pipeline == pipe && o.Operator == op {
				return o
			}
		}
	}
	t.Fatalf("健康汇总中找不到 %s/%s", pipe, op)
	return OperatorHealth{}
}

// TestHealthMatchesActiveAlerts 健康汇总与逐条接入判定、ActiveAlerts
// 完全对得上：卡死/滞后标记、活动告警数、原因集合逐一核对。
func TestHealthMatchesActiveAlerts(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, stuckCfg(), clk, nil)

	// p/agg：任务卡死 + 数据滞后；p/sink：任务失败；p/ok：一切正常。
	ingest(t, e, Event{ID: "a1", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
	ingest(t, e, Event{ID: "k1", Pipeline: "p", Operator: "ok", Kind: KindRecord, OccurredAt: t0})
	ingest(t, e, Event{ID: "s1", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T9", OccurredAt: t0})
	ingest(t, e, Event{ID: "f1", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T9", OccurredAt: t0.Add(time.Second), Failed: true, ErrorMessage: "boom"})

	clk.Set(t0.Add(60 * time.Second))
	r := observe(t, e)
	if !hasCause(r.Alerts, CauseStuck) {
		t.Fatal("前置：agg 应已卡死")
	}
	// ok 算子补一条新数据，保持新鲜。
	ingest(t, e, Event{ID: "k2", Pipeline: "p", Operator: "ok", Kind: KindRecord, OccurredAt: t0.Add(60 * time.Second)})

	hs := e.Health()
	agg := findOp(t, hs, "p", "agg")
	if !agg.Stuck || !agg.Stale {
		t.Fatalf("agg 应同时卡死+滞后，got %+v", agg)
	}
	if agg.RunningTasks != 1 {
		t.Fatalf("agg 应有 1 个在途任务，got %d", agg.RunningTasks)
	}
	sink := findOp(t, hs, "p", "sink")
	if sink.Stuck {
		t.Fatalf("sink 任务已结束，不应卡死，got %+v", sink)
	}
	if !sink.Stale {
		t.Fatalf("sink 60s 无新接入，应滞后，got %+v", sink)
	}
	ok := findOp(t, hs, "p", "ok")
	if ok.Stuck || ok.Stale || ok.ActiveAlerts != 0 {
		t.Fatalf("ok 应完全健康，got %+v", ok)
	}

	// 与 ActiveAlerts 严格对账：每个算子的 firing 数、原因集合一致。
	active := e.ActiveAlerts()
	wantByOp := map[string]int{}
	for _, a := range active {
		wantByOp[a.Pipeline+"|"+a.Operator]++
	}
	total := 0
	for _, p := range hs {
		for _, o := range p.Operators {
			total += o.ActiveAlerts
			if want := wantByOp[o.Pipeline+"|"+o.Operator]; o.ActiveAlerts != want {
				t.Fatalf("%s/%s 活动告警数=%d，ActiveAlerts 口径=%d", o.Pipeline, o.Operator, o.ActiveAlerts, want)
			}
		}
	}
	if total != len(active) {
		t.Fatalf("健康汇总告警总数=%d，ActiveAlerts=%d", total, len(active))
	}
	t.Logf("健康汇总与 ActiveAlerts 对账一致：%d 条活动告警", total)
}

// TestHealthConsistentAfterOutOfOrderAndRestart 乱序/延迟到达与重启恢复后，
// 健康汇总仍与告警状态一致，不各说各话。
func TestHealthConsistentAfterOutOfOrderAndRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	cfg := stuckCfg()

	func() {
		clk := NewMockClock(t0)
		e := testEngine(t, cfg, clk, NewFileStore(path))
		// 乱序：finished 先于 started 到达。
		ingest(t, e, Event{ID: "f-late", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "TX", OccurredAt: t0.Add(10 * time.Second), Failed: true, ErrorMessage: "boom"})
		ingest(t, e, Event{ID: "s-late", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "TX", OccurredAt: t0})
		// 另一算子任务卡死。
		ingest(t, e, Event{ID: "a1", Pipeline: "p", Operator: "agg", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
		clk.Set(t0.Add(60 * time.Second))
		observe(t, e)
		if err := e.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	clk := NewMockClock(t0.Add(70 * time.Second))
	e2, err := Open(context.Background(), cfg, clk, NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	e2.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = e2.Close(context.Background()) })

	hs := e2.Health()
	sink := findOp(t, hs, "p", "sink")
	if sink.RunningTasks != 0 {
		t.Fatalf("TX 已结束（乱序到达），在途任务应为 0，got %d", sink.RunningTasks)
	}
	agg := findOp(t, hs, "p", "agg")
	if !agg.Stuck {
		t.Fatal("重启后 agg 卡死状态必须延续到健康汇总")
	}
	// 对账：汇总总数 == ActiveAlerts 数。
	total := 0
	for _, p := range hs {
		total += p.ActiveAlerts
	}
	if total != len(e2.ActiveAlerts()) {
		t.Fatalf("重启后汇总=%d 与 ActiveAlerts=%d 不一致", total, len(e2.ActiveAlerts()))
	}
	t.Log("乱序+重启后健康汇总与告警状态一致")
}
