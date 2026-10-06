package pipelineobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestOperatorScopeSilenceCoversExistingAndNew 算子范围静默：一次静默
// 该算子名下所有类型问题——已正在报的与静默期内新冒出来的都压住，
// 但不影响其他算子；内部状态机照常推进。
func TestOperatorScopeSilenceCoversExistingAndNew(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, stuckCfg(), clk, nil)

	// op 已滞后（正在报）。
	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	clk.Set(t0.Add(30 * time.Second))
	if r := observe(t, e); !hasCause(r.Alerts, CauseStale) {
		t.Fatal("前置：op 应已滞后告警")
	}

	// 按算子范围静默（p/op 名下全部问题），持续 10 分钟。
	id, err := e.SilenceScoped(SilenceScope{Pipeline: "p", Operator: "op"}, 10*time.Minute, "maintenance")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("已加算子范围静默 id=%s", id)

	// 静默期内：op 的任务卡死（新冒出来的问题）不对外通知；
	// 但另一个算子 op2 的滞后照常告警（范围外不受影响）。
	ingest(t, e, Event{ID: "s1", Pipeline: "p", Operator: "op", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0.Add(30 * time.Second)})
	ingest(t, e, Event{ID: "y1", Pipeline: "p", Operator: "op2", Kind: KindRecord, OccurredAt: t0.Add(31 * time.Second)})
	clk.Set(t0.Add(91 * time.Second)) // op: 滞后+卡死均成立；op2: 滞后 60s 成立
	r := observe(t, e)
	for _, a := range r.Alerts {
		if a.Operator == "op" {
			t.Fatalf("静默期内 op 不应产生对外告警，got %+v", a)
		}
	}
	if !hasCause(r.Alerts, CauseStale) {
		t.Fatal("范围外的 op2 滞后应照常告警")
	}
	// 内部状态照常推进：op 的滞后与卡死事件单都在 firing。
	keys := firingKeys(e)
	if !keys[alertKey("p", "op", CauseStale)] || !keys[alertKey("p", "op", CauseStuck)] {
		t.Fatalf("静默只压通知，op 的事件单应照常 firing，got %v", keys)
	}
	t.Log("算子范围静默：已报+新冒问题都压住，范围外不受影响，状态机照常推进")
}

// TestPipelineScopeSilenceAndExpiryReAlert 整条流水线范围静默；
// 到期后只要问题还在，必须重新发出告警——即使该轮次以前报过一次。
func TestPipelineScopeSilenceAndExpiryReAlert(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	clk.Set(t0.Add(30 * time.Second))
	r := observe(t, e)
	if !hasCause(r.Alerts, CauseStale) {
		t.Fatal("前置：应已滞后告警（该轮次已通知过一次）")
	}

	// 整条流水线静默 1 分钟。
	if _, err := e.SilenceScoped(SilenceScope{Pipeline: "p"}, time.Minute, "pipeline maintenance"); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(45 * time.Second))
	if r := observe(t, e); len(r.Alerts) != 0 {
		t.Fatalf("流水线静默期内不应有告警，got %+v", r.Alerts)
	}

	// 静默到期（now >= Until 即失效）：问题仍在，必须重新发出告警，
	// 不能因为“以前报过一次”就不出声。
	clk.Set(t0.Add(91 * time.Second))
	r = observe(t, e)
	if !hasCause(r.Alerts, CauseStale) {
		t.Fatalf("静默到期后问题仍在，必须重新告警，got %+v", r.Alerts)
	}
	t.Log("流水线范围静默到期 -> 问题仍在即重新报警（不受历史通知影响）")
}

// TestScopedSilenceManualRemoveReAlert 手动解除范围静默后，
// 仍在 firing 的问题在下一次评估重新报警。
func TestScopedSilenceManualRemoveReAlert(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	id, err := e.SilenceScoped(SilenceScope{Pipeline: "p", Operator: "op", Cause: CauseStale}, 0, "permanent")
	if err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(30 * time.Second))
	if r := observe(t, e); len(r.Alerts) != 0 {
		t.Fatalf("静默期内不应告警，got %+v", r.Alerts)
	}

	// 手动解除 -> 下一次评估立即补发。
	if err := e.RemoveSilenceRule(id); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(31 * time.Second))
	if r := observe(t, e); !hasCause(r.Alerts, CauseStale) {
		t.Fatal("手动解除静默后应重新报警")
	}
	if len(e.ListSilences()) != 0 {
		t.Fatal("规则已移除，ListSilences 应为空")
	}
	t.Log("手动解除范围静默 -> 问题仍在即重新报警")
}

// TestOverlappingScopesDeterministic 多种范围同时命中时行为唯一确定：
// 任一命中即静默（并集）；移除较宽范围后，较窄范围仍然生效。
func TestOverlappingScopesDeterministic(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	// 同时存在：整条流水线静默 + 单算子静默 + 单次问题精确静默。
	pipeID, _ := e.SilenceScoped(SilenceScope{Pipeline: "p"}, 0, "pipeline-wide")
	opID, _ := e.SilenceScoped(SilenceScope{Pipeline: "p", Operator: "op"}, 0, "operator-wide")
	if err := e.Silence("p", "op", CauseStale, 0, "exact"); err != nil {
		t.Fatal(err)
	}

	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	clk.Set(t0.Add(30 * time.Second))
	if r := observe(t, e); len(r.Alerts) != 0 {
		t.Fatalf("多层静默命中时不应告警，got %+v", r.Alerts)
	}

	// 移除流水线级与算子级后，精确根因静默仍然生效（各自独立、确定性）。
	if err := e.RemoveSilenceRule(pipeID); err != nil {
		t.Fatal(err)
	}
	if err := e.RemoveSilenceRule(opID); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(31 * time.Second))
	if r := observe(t, e); len(r.Alerts) != 0 {
		t.Fatalf("精确静默仍应生效，got %+v", r.Alerts)
	}

	// 全部移除后才重新报警。
	if err := e.RemoveSilence("p", "op", CauseStale); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(32 * time.Second))
	if r := observe(t, e); !hasCause(r.Alerts, CauseStale) {
		t.Fatal("全部静默移除后应重新报警")
	}
	t.Log("多范围同时命中：并集生效、逐层移除行为确定")
}

// TestScopedSilenceSurvivesRestart 范围静默规则跨重启延续，
// 重启后继续压住范围内的新问题。
func TestScopedSilenceSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	cfg := testCfg()

	func() {
		clk := NewMockClock(t0)
		e := testEngine(t, cfg, clk, NewFileStore(path))
		if _, err := e.SilenceScoped(SilenceScope{Pipeline: "p", Operator: "op"}, 0, "persistent"); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	clk := NewMockClock(t0)
	e2, err := Open(context.Background(), cfg, clk, NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	e2.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = e2.Close(context.Background()) })

	if got := len(e2.ListSilences()); got != 1 {
		t.Fatalf("重启后范围静默应延续，got %d 条", got)
	}
	ingest(t, e2, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	clk.Set(t0.Add(time.Hour))
	if r := observe(t, e2); len(r.Alerts) != 0 {
		t.Fatalf("重启后范围静默应继续生效，got %+v", r.Alerts)
	}
	if len(e2.ActiveAlerts()) != 1 {
		t.Fatal("静默只压通知，事件单应照常 firing")
	}
	t.Log("范围静默跨重启延续：通知被压住、状态机照常推进")
}

// TestOperatorScopeSilenceCoversTaskFailures 算子范围静默覆盖该算子名下
// 所有任务失败（每个任务实例是独立根因键），静默期内新失败不通知；
// 到期后新失败正常告警。
func TestOperatorScopeSilenceCoversTaskFailures(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	if _, err := e.SilenceScoped(SilenceScope{Pipeline: "p", Operator: "sink"}, time.Minute, "sink maintenance"); err != nil {
		t.Fatal(err)
	}
	// 静默期内两个任务先后失败：都不对外通知。
	ingest(t, e, Event{ID: "s1", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
	r := ingest(t, e, Event{ID: "f1", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(time.Second), Failed: true, ErrorMessage: "boom"})
	if len(r.Alerts) != 0 {
		t.Fatalf("静默期内任务失败不应通知，got %+v", r.Alerts)
	}
	ingest(t, e, Event{ID: "s2", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T2", OccurredAt: t0.Add(2 * time.Second)})
	r = ingest(t, e, Event{ID: "f2", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T2", OccurredAt: t0.Add(3 * time.Second), Failed: true, ErrorMessage: "boom"})
	if len(r.Alerts) != 0 {
		t.Fatalf("静默期内第二个任务失败也不应通知，got %+v", r.Alerts)
	}
	// 内部状态照常：两条失败事件单都在 firing。
	if len(e.ActiveAlerts()) != 2 {
		t.Fatalf("两次失败的事件单都应 firing，got %d", len(e.ActiveAlerts()))
	}

	// 静默到期后：静默期内未通知的失败在下一次评估补发；
	// 之后新任务失败正常即时告警。
	clk.Set(t0.Add(61 * time.Second))
	r = observe(t, e)
	failed := 0
	for _, a := range r.Alerts {
		if a.Cause == CauseFailed {
			failed++
		}
	}
	if failed != 2 {
		t.Fatalf("静默到期后应补发 2 条被压住的失败告警，got %d: %+v", failed, r.Alerts)
	}
	ingest(t, e, Event{ID: "s3", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T3", OccurredAt: t0.Add(62 * time.Second)})
	r = ingest(t, e, Event{ID: "f3", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T3", OccurredAt: t0.Add(63 * time.Second), Failed: true, ErrorMessage: "boom"})
	if !hasCause(r.Alerts, CauseFailed) {
		t.Fatal("静默到期后新失败应正常告警")
	}
	t.Log("算子范围静默覆盖全部任务失败；到期补发被压告警，新失败即时告警")
}
