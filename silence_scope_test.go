package pipelineobs

import (
	"context"
	"testing"
	"time"
)

// makeStuck 启动任务并把观察时刻向前推过卡住阈值，制造一条外发的 firing 卡住。
// 基于当前时钟只向前推进，避免与单调有效时间冲突。
func makeStuck(t *testing.T, e *Engine, pipe, op, task string) {
	t.Helper()
	clk, ok := e.clock.(*MockClock)
	if !ok {
		t.Fatalf("需要 MockClock")
	}
	base := clk.Now()
	ingest(t, e, Event{ID: task + "-start", Pipeline: pipe, Operator: op, Kind: KindTaskStarted, TaskID: task, OccurredAt: base})
	clk.Advance(e.cfg.TaskStuckThreshold)
	res, err := e.ObserveTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("前置：%s/%s 应已卡住，got %+v", pipe, op, res.Alerts)
	}
}

// makeStuckSuppressed 在已有（针对卡住的）静默下制造卡住：卡住不外发、
// 但内部 firing。其它未被该静默覆盖的原因（如数据滞后）允许照常外发。
func makeStuckSuppressed(t *testing.T, e *Engine, pipe, op, task string) {
	t.Helper()
	clk := e.clock.(*MockClock)
	base := clk.Now()
	ingest(t, e, Event{ID: task + "-start", Pipeline: pipe, Operator: op, Kind: KindTaskStarted, TaskID: task, OccurredAt: base})
	clk.Advance(e.cfg.TaskStuckThreshold)
	res, _ := e.ObserveTime(context.Background())
	if hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("%s/%s 的卡住被静默，不应外发，got %+v", pipe, op, res.Alerts)
	}
	if !firingKeys(e)[alertKey(pipe, op, CauseStuck)] {
		t.Fatalf("%s/%s 内部应已 firing", pipe, op)
	}
}

// TestOperatorSilenceCoversWholeClass 单算子静默一次压住该算子名下
// “一整类问题”：包括已经在报的、以及静默期内新冒出的同类/不同类问题；
// 内部状态机仍推进并持久化（ActiveAlerts 可见）。
func TestOperatorSilenceCoversWholeClass(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	makeStuck(t, e, "p1", "agg", "T1") // agg 已有一条 firing 卡住

	// 对 agg 做算子级静默（不区分原因，所有失败/卡住/滞后都被覆盖）。
	if err := e.SilenceOperator("p1", "agg", time.Minute, "ops-maint"); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(60 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	for _, a := range res.Alerts {
		if a.Pipeline == "p1" && a.Operator == "agg" {
			t.Fatalf("算子静默期内 agg 不得对外告警，got %+v", a)
		}
	}
	// 静默只压通知：已在报的卡住仍 firing。
	if !firingKeys(e)[alertKey("p1", "agg", CauseStuck)] {
		t.Fatal("静默期内卡住事件单应仍为 firing（内部状态照常推进）")
	}

	// 静默期内该算子再冒出一个失败任务（新根因）：同样被压住，但内部建档 firing。
	ingest(t, e, Event{ID: "s2", Pipeline: "p1", Operator: "agg", Kind: KindTaskStarted, TaskID: "T2", OccurredAt: t0.Add(60 * time.Second)})
	r := ingest(t, e, Event{ID: "f2", Pipeline: "p1", Operator: "agg", Kind: KindTaskFinished, TaskID: "T2", OccurredAt: t0.Add(61 * time.Second), Failed: true, ErrorMessage: "boom"})
	for _, a := range r.Alerts {
		if a.Pipeline == "p1" && a.Operator == "agg" {
			t.Fatalf("静默期内新冒出的失败也不得对外告警，got %+v", a)
		}
	}
	if !firingKeys(e)[failureAlertKey("p1", "agg", "T2")] {
		t.Fatal("静默期内新失败必须在内部建档为 firing，只是不通知")
	}

	// 其它算子的问题不受算子静默影响（范围口径唯一，不扩大）。
	makeStuck(t, e, "p1", "other", "TX")
	// makeStuck 已断言 other 的卡住告警真实发出。
	t.Log("算子级静默：压住该算子已在报+新冒出问题，其它算子不受影响，内部 firing 照常")
}

// TestPipelineSilenceCoversAllOperators 整条流水线静默覆盖其名下所有算子；
// 另一条流水线不受影响。
func TestPipelineSilenceCoversAllOperators(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	if err := e.SilencePipeline("p1", 0, "pipeline-maint"); err != nil {
		t.Fatal(err)
	}
	// p1 下两个算子都卡住：均不对外通知，但内部 firing。
	makeStuckSuppressed(t, e, "p1", "a", "T1")
	makeStuckSuppressed(t, e, "p1", "b", "T2")
	// p2 同名算子不受影响，正常报出。
	makeStuck(t, e, "p2", "a", "T3")
	t.Log("流水线级静默覆盖 p1.* 全部算子；p2 不受影响")
}

// TestSilenceExpiryReAlertsPersistingProblem 静默到期后，只要问题还在，
// 必须重新发出告警；到期边界 now >= Until 当次即生效。
func TestSilenceExpiryReAlertsPersistingProblem(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	makeStuck(t, e, "p1", "agg", "T1") // 已发出一条卡住

	if err := e.SilenceOperator("p1", "agg", 30*time.Second, "maint"); err != nil {
		t.Fatal(err)
	}
	// 静默中持续观察：不发。
	clk.Set(t0.Add(60 * time.Second))
	if res, _ := e.ObserveTime(context.Background()); len(res.Alerts) != 0 {
		t.Fatalf("静默期内不应外发，got %+v", res.Alerts)
	}

	// 到期边界：Until = 00:45+30s = 01:15；now==01:15 当次即失效并重新报警。
	// 算子静默同时覆盖卡住与滞后，二者都在持续 => 都重新报警。
	clk.Set(t0.Add(75 * time.Second))
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("静默到期且问题仍在，必须重新报卡住，got %+v", res.Alerts)
	}
	t.Logf("静默到期（now>=Until）当次重新报警：%d 条（含卡住）", len(res.Alerts))

	// 再下一次观察：同一轮不重复刷屏。
	clk.Set(t0.Add(80 * time.Second))
	if res, _ := e.ObserveTime(context.Background()); len(res.Alerts) != 0 {
		t.Fatalf("重新报过之后同轮不得刷屏，got %+v", res.Alerts)
	}
}

// TestSilenceManualRemoveReAlerts 手动解除静默后，下一次评估对持续中的
// 问题重新报警；并且“新一轮恶化”（先恢复再卡住）也能再次报出。
func TestSilenceManualRemoveReAlerts(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	makeStuck(t, e, "p1", "agg", "T1")

	if err := e.SilenceOperator("p1", "agg", 0, "maint"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(15 * time.Second)
	if res, _ := e.ObserveTime(context.Background()); len(res.Alerts) != 0 {
		t.Fatalf("静默中不发，got %+v", res.Alerts)
	}

	if err := e.RemoveSilenceOperator("p1", "agg"); err != nil {
		t.Fatal(err)
	}
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("手动解除后持续中的问题应重新报警，got %+v", res.Alerts)
	}
	t.Log("手动解除静默 => 持续中的卡住重新发出告警")

	// 任务恢复进展、卡住恢复；之后再次卡住（新一轮恶化）：静默已不存在，必须报。
	now := clk.Now()
	ingest(t, e, Event{ID: "rec", Pipeline: "p1", Operator: "agg", Kind: KindRecord, OccurredAt: now})
	clk.Advance(e.cfg.TaskStuckThreshold)
	res, _ = e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("解除静默后的新一轮恶化必须报出，got %+v", res.Alerts)
	}
	t.Log("解除后再经历恢复-再卡住：新一轮恶化正常报出，不被历史静默吞掉")
}

// TestSilencePriorityIsUniform 全局 / 流水线 / 算子 / 单问题多范围同时命中时，
// 结论唯一：只要任一覆盖就压通知；只剩更窄规则时仍压；全部解除才恢复通知。
func TestSilencePriorityIsUniform(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	makeStuck(t, e, "p1", "agg", "T1")

	// 同时挂三层：全局、流水线、算子。
	if err := e.Silence("", "", "", time.Hour, "g"); err != nil {
		t.Fatal(err)
	}
	if err := e.SilencePipeline("p1", time.Hour, "p"); err != nil {
		t.Fatal(err)
	}
	if err := e.SilenceOperator("p1", "agg", time.Hour, "o"); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(60 * time.Second))
	if res, _ := e.ObserveTime(context.Background()); len(res.Alerts) != 0 {
		t.Fatalf("三层同时命中应压通知，got %+v", res.Alerts)
	}

	// 去掉全局：流水线+算子仍覆盖。
	if err := e.RemoveSilence("", "", ""); err != nil {
		t.Fatal(err)
	}
	if res, _ := e.ObserveTime(context.Background()); len(res.Alerts) != 0 {
		t.Fatalf("流水线/算子仍覆盖，不应恢复通知，got %+v", res.Alerts)
	}
	// 再去流水线：算子仍覆盖。
	if err := e.RemoveSilencePipeline("p1"); err != nil {
		t.Fatal(err)
	}
	if res, _ := e.ObserveTime(context.Background()); len(res.Alerts) != 0 {
		t.Fatalf("算子规则仍覆盖，不应恢复通知，got %+v", res.Alerts)
	}
	// 最后去算子：覆盖清零，持续问题重新报警（卡住必在其中）。
	if err := e.RemoveSilenceOperator("p1", "agg"); err != nil {
		t.Fatal(err)
	}
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("全部静默解除后持续中的卡住应重新报出，got %+v", res.Alerts)
	}
	t.Log("多范围同时命中：逐层解除，覆盖结论始终唯一，全清才重新报警")
}

// TestCauseSilenceDoesNotLeak 单原因静默只压该算子该原因，同算子其它原因、
// 其它算子的问题照常报。
func TestCauseSilenceDoesNotLeak(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	// 只静默 agg 的卡住。
	if err := e.Silence("p1", "agg", CauseStuck, 0, "only-stuck"); err != nil {
		t.Fatal(err)
	}
	// agg 卡住被压（内部 firing，不外发）。
	makeStuckSuppressed(t, e, "p1", "agg", "T1") // 时钟推进到 t0+45s

	// 另一个算子失败不受影响（不同算子），事件时间随当前时钟。
	base := clk.Now()
	ingest(t, e, Event{ID: "s2", Pipeline: "p1", Operator: "sink", Kind: KindTaskStarted, TaskID: "T2", OccurredAt: base})
	r := ingest(t, e, Event{ID: "f2", Pipeline: "p1", Operator: "sink", Kind: KindTaskFinished, TaskID: "T2", OccurredAt: base.Add(2 * time.Second), Failed: true, ErrorMessage: "x"})
	if !hasCause(r.Alerts, CauseFailed) {
		t.Fatalf("单原因静默不得扩大到其它算子，失败应正常报出，got %+v", r.Alerts)
	}

	// 同算子 agg 的数据滞后也不被“卡住静默”压住：再推进越过滞后阈值。
	clk.Advance(e.cfg.StalenessThreshold)
	res, _ := e.ObserveTime(context.Background())
	if !hasCause(res.Alerts, CauseStale) {
		t.Fatalf("只静默卡住，同算子数据滞后应照常报，got %+v", res.Alerts)
	}
	if hasCause(res.Alerts, CauseStuck) {
		t.Fatalf("agg 的卡住仍应被压住，got %+v", res.Alerts)
	}
	if !firingKeys(e)[alertKey("p1", "agg", CauseStuck)] {
		t.Fatal("卡住虽不外发，内部仍应为 firing")
	}
	t.Log("单原因静默不泄漏到同算子其它原因、也不扩大到其它算子")
}
