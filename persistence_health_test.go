package pipelineobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// reopenEngine 关闭并用同一文件、新时钟重新打开引擎，模拟进程重启。
func reopenEngine(t *testing.T, cfg Config, clk *MockClock, path string) *Engine {
	t.Helper()
	store := NewFileStore(path)
	e, err := Open(context.Background(), cfg, clk, store)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	e.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

// TestStuckRestartContinuesWithoutSpam 卡住 firing 后进程重启：
// 卡住状态延续（不丢），再次评估不重复刷屏；任务恢复进展后卡住恢复；
// 之后再次卡住仍能重新报出。静默规则也随重启延续。
func TestStuckRestartContinuesWithoutSpam(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	cfg := testCfg()

	func() {
		clk := NewMockClock(t0)
		e := testEngine(t, cfg, clk, NewFileStore(path))
		makeStuck(t, e, "p1", "agg", "T1") // 卡住已外发并持久化
		// 算子级静默也持久化。
		if err := e.SilencePipeline("p9", time.Hour, "persisted"); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// 重启：时钟停在卡住时刻之后。
	clk := NewMockClock(t0.Add(cfg.TaskStuckThreshold))
	e2 := reopenEngine(t, cfg, clk, path)

	keys := firingKeys(e2)
	if !keys[alertKey("p1", "agg", CauseStuck)] {
		t.Fatalf("重启后卡住状态必须延续，实际 firing=%v", keys)
	}
	t.Log("重启后卡住事件单原样恢复（firing 延续，未丢失）")

	// 再评估：同一轮不得重复外发（不刷屏）。
	res, _ := e2.ObserveTime(context.Background())
	if hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("重启后重复评估不得再次外发卡住，got %+v", res.Alerts)
	}

	// 持久化的静默规则延续：p9 卡住应被压。
	makeStuckSuppressed(t, e2, "p9", "agg", "T9")
	t.Log("重启后算子/流水线静默规则继续生效")

	// 恢复进展 -> 卡住恢复。
	base := clk.Now()
	r := ingest(t, e2, Event{ID: "rec", Pipeline: "p1", Operator: "agg", Kind: KindRecord, OccurredAt: base})
	found := false
	for _, a := range r.Recovered {
		if a.Cause == CauseStuck {
			found = true
		}
	}
	if !found {
		t.Fatalf("重启后任务恢复进展，卡住应恢复，got %+v", r.Recovered)
	}

	// 再次卡住：新一轮必须能重新报。
	clk.Advance(cfg.TaskStuckThreshold)
	res, _ = e2.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("恢复后再次卡住应重新报出，got %+v", res.Alerts)
	}
	t.Log("重启后：卡住不刷屏、可恢复、可再次报出；静默规则延续")
}

// TestV1SnapshotSilenceMigration 手工构造 v1 格式快照，验证加载时把旧
// 键值静默迁移为结构化范围规则，且旧的全局/精确/失败键语义保持。
func TestV1SnapshotSilenceMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v1.json")
	store := NewFileStore(path)

	// 直接构造一个 v1 快照。
	old := &snapshot{
		Version:       1,
		Silences:      map[string]silenceState{},
		Buckets:       map[string]*bucketState{},
		Tasks:         map[string]*taskState{},
		Incidents:     map[string]*incidentState{},
		SeenEvents:    map[string]time.Time{},
		ExpiredEvents: map[string]time.Time{},
		Watermarks:    map[string]time.Time{},
		LastIngest:    map[string]time.Time{},
		EffectiveNow:  t0,
	}
	old.Silences["*"] = silenceState{Note: "g"}
	old.Silences["p1|agg|data_stale"] = silenceState{Until: t0.Add(time.Hour), Note: "c"}
	old.Silences["p1|sink|task_failed|T7"] = silenceState{Note: "f"}
	if err := store.Save(context.Background(), old); err != nil {
		t.Fatal(err)
	}

	cfg := testCfg()
	clk := NewMockClock(t0)
	e := reopenEngine(t, cfg, clk, path)

	rules := map[string]SilenceView{}
	for _, v := range e.ListSilences() {
		rules[v.ID] = v
	}
	if _, ok := rules["global"]; !ok {
		t.Fatalf("v1 全局静默应迁移，实际 %v", rules)
	}
	c, ok := rules["cause:p1|agg|data_stale"]
	if !ok || c.Scope != ScopeCause || c.Pipeline != "p1" || c.Operator != "agg" || c.Cause != CauseStale {
		t.Fatalf("v1 精确键迁移错误：%+v", c)
	}
	f, ok := rules["task:p1|sink|task_failed|T7"]
	if !ok || f.Scope != ScopeCause || f.TaskID != "T7" || f.Cause != CauseFailed {
		t.Fatalf("v1 失败键迁移错误：%+v", f)
	}
	t.Log("v1 快照静默规则成功迁移为 global/cause/task 三类结构化规则")

	// 迁移后另存为 v2，再重开不报错。
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e2 := reopenEngine(t, cfg, NewMockClock(t0), path)
	if len(e2.ListSilences()) != 3 {
		t.Fatalf("v2 重开后规则应保持 3 条，got %d", len(e2.ListSilences()))
	}
}

// TestHealthSummaryMatchesActiveAlerts 健康汇总与逐条判定、活动告警严格对账：
// 卡住/滞滞标志、各算子告警计数、原因集合与 ActiveAlerts 完全一致；
// 无告警算子也出现（健康形态）；恢复后标志随之清除。
func TestHealthSummaryMatchesActiveAlerts(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	// p1/agg：一个任务起后卡住（45s），同刻也滞后。
	makeStuck(t, e, "p1", "agg", "T1")
	// p1/sink：一个失败任务 + 滞后之外不卡住。
	base := clk.Now()
	ingest(t, e, Event{ID: "fs", Pipeline: "p1", Operator: "sink", Kind: KindTaskStarted, TaskID: "T2", OccurredAt: base})
	ingest(t, e, Event{ID: "ff", Pipeline: "p1", Operator: "sink", Kind: KindTaskFinished, TaskID: "T2", OccurredAt: base.Add(time.Second), Failed: true, ErrorMessage: "boom"})
	// p2/src：只接入一条记录，健康。
	ingest(t, e, Event{ID: "ok", Pipeline: "p2", Operator: "src", Kind: KindRecord, OccurredAt: base})

	summary := e.HealthSummary()
	active := e.ActiveAlerts()

	// 总 firing 数必须与活动告警总条数逐一对账。
	total := 0
	for _, ph := range summary {
		total += ph.FiringCount
	}
	if total != len(active) {
		t.Fatalf("健康汇总 firing 总数 %d 与 ActiveAlerts %d 不一致", total, len(active))
	}

	findOp := func(sum []PipelineHealth, pipe, op string) *OperatorHealth {
		for _, ph := range sum {
			if ph.Pipeline != pipe {
				continue
			}
			for i := range ph.Operators {
				if ph.Operators[i].Operator == op {
					return &ph.Operators[i]
				}
			}
		}
		return nil
	}

	agg := findOp(summary, "p1", "agg")
	if agg == nil || !agg.Stuck || !agg.Stale {
		t.Fatalf("p1/agg 应卡住且滞后，got %+v", agg)
	}
	sink := findOp(summary, "p1", "sink")
	if sink == nil || sink.Stuck {
		t.Fatalf("p1/sink 不应卡住，got %+v", sink)
	}
	src := findOp(summary, "p2", "src")
	if src == nil || src.FiringCount != 0 || src.Stuck || src.Stale {
		t.Fatalf("p2/src 应健康且仍出现在汇总，got %+v", src)
	}
	// 原因集合必须含 task_stuck。
	var hasStuckCause bool
	for _, c := range agg.Causes {
		if c == CauseStuck {
			hasStuckCause = true
		}
	}
	if !hasStuckCause {
		t.Fatalf("agg 原因集合应含 task_stuck，got %v", agg.Causes)
	}
	t.Logf("健康汇总与活动告警对账一致：总 firing=%d，p1/agg={stuck,stale}，p2/src 健康", total)

	// agg 任务恢复进展后，卡住与滞后标志应在下一轮接入后清除（与状态机同源）。
	clk.Advance(time.Second)
	ingest(t, e, Event{ID: "rec", Pipeline: "p1", Operator: "agg", Kind: KindRecord, OccurredAt: clk.Now()})

	summary2 := e.HealthSummary()
	if total2 := func() int {
		n := 0
		for _, ph := range summary2 {
			n += ph.FiringCount
		}
		return n
	}(); total2 != len(e.ActiveAlerts()) {
		t.Fatalf("恢复后汇总 firing=%d 与活动告警 %d 不一致", total2, len(e.ActiveAlerts()))
	}
	agg2 := findOp(summary2, "p1", "agg")
	if agg2 == nil || agg2.Stuck || agg2.Stale {
		t.Fatalf("恢复后健康汇总应同步清除卡住/滞后，got %+v", agg2)
	}
	t.Log("恢复后健康汇总同步更新，仍与活动告警严格对账")
}

// TestHealthSummaryConsistentAcrossRestart 重启恢复后健康汇总仍与
// 活动告警一致（不各说各话）。
func TestHealthSummaryConsistentAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.json")
	cfg := testCfg()
	func() {
		clk := NewMockClock(t0)
		e := testEngine(t, cfg, clk, NewFileStore(path))
		makeStuck(t, e, "p1", "agg", "T1")
		_ = e.Close(context.Background())
	}()
	clk := NewMockClock(t0.Add(cfg.TaskStuckThreshold))
	e2 := reopenEngine(t, cfg, clk, path)

	sum := 0
	var stuck bool
	for _, ph := range e2.HealthSummary() {
		sum += ph.FiringCount
		if ph.Stuck {
			stuck = true
		}
	}
	if !stuck {
		t.Fatal("重启后汇总应仍显示卡住")
	}
	if sum != len(e2.ActiveAlerts()) {
		t.Fatalf("重启后汇总计数 %d 与活动告警 %d 不一致", sum, len(e2.ActiveAlerts()))
	}
	t.Logf("重启后健康汇总与活动告警一致：firing=%d stuck=true", sum)
}
