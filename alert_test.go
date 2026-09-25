package pipelineobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestSuppressionSingleAlert 同一根因在持续恶化期间只产生一条有效告警。
func TestSuppressionSingleAlert(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})

	clk.Set(t0.Add(30 * time.Second))
	r1, _ := e.ObserveTime(context.Background())
	clk.Set(t0.Add(3 * time.Minute))
	r2, _ := e.ObserveTime(context.Background())
	clk.Set(t0.Add(30 * time.Minute))
	r3, _ := e.ObserveTime(context.Background())

	total := len(r1.Alerts) + len(r2.Alerts) + len(r3.Alerts)
	if total != 1 {
		t.Fatalf("同一 firing 轮次应只产生 1 条告警，实际 %d", total)
	}
	t.Log("同一根因持续恶化只产生一条告警（跨抑制窗口亦不刷屏）")
}

// TestSilenceSuppressesAndExpires 静默期内不对外告警；静默到期后恢复通知能力。
func TestSilenceSuppressesAndExpires(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})

	// 给该根因加 1 分钟静默。
	if err := e.Silence("p", "op", CauseStale, time.Minute, "maintenance"); err != nil {
		t.Fatal(err)
	}
	clk.Set(t0.Add(30 * time.Second))
	r, _ := e.ObserveTime(context.Background())
	if len(r.Alerts) != 0 {
		t.Fatalf("静默期内不应产生对外告警，got %+v", r.Alerts)
	}
	if len(e.ActiveAlerts()) != 1 {
		t.Fatalf("静默只抑制通知，状态机仍应为 firing，got %d", len(e.ActiveAlerts()))
	}

	// 静默到期后：当前轮次已 Notified=false（静默时未置位），
	// 下一次评估应补发这一轮的一条告警。
	clk.Set(t0.Add(61 * time.Second))
	r, _ = e.ObserveTime(context.Background())
	if len(r.Alerts) != 1 || r.Alerts[0].Cause != CauseStale {
		t.Fatalf("静默到期后应能产生 1 条告警，got %+v", r.Alerts)
	}
	t.Log("静默抑制通知且到期后恢复通知能力，状态机全程延续")
}

// TestFailureAlertOncePerTask 同一任务失败只告警一次，重放 finished 不重复；
// 不同任务实例失败各自产生告警。
func TestFailureAlertOncePerTask(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)
	ingest(t, e, Event{ID: "s1", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
	r := ingest(t, e, Event{ID: "f1", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(5 * time.Second), Failed: true, ErrorMessage: "boom"})
	if !hasCause(r.Alerts, CauseFailed) {
		t.Fatal("首次失败应告警")
	}
	// 重放同一 finished（重复 ID）-> duplicate；
	dup := ingest(t, e, Event{ID: "f1", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(5 * time.Second), Failed: true, ErrorMessage: "boom"})
	if !dup.Verdict.Duplicate {
		t.Fatal("重放应判重")
	}
	// 即便一个新 ID 的 finished 再来一次（异常重投），任务已 FailAlerted，也不重复告警。
	r2 := ingest(t, e, Event{ID: "f1-again", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(6 * time.Second), Failed: true, ErrorMessage: "boom"})
	for _, a := range r2.Alerts {
		if a.Cause == CauseFailed {
			t.Fatal("同一任务实例的失败不得重复告警")
		}
	}

	// 新任务实例失败 -> 新根因键，允许再次告警。
	ingest(t, e, Event{ID: "s2", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T2", OccurredAt: t0.Add(10 * time.Second)})
	r3 := ingest(t, e, Event{ID: "f2", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T2", OccurredAt: t0.Add(15 * time.Second), Failed: true, ErrorMessage: "again"})
	if !hasCause(r3.Alerts, CauseFailed) {
		t.Fatal("新任务实例失败应能重新告警")
	}
}

// TestRestartContinuesAlertsAndDedup 模拟进程重启：
// 未恢复告警、静默状态、去重信息全部延续；不重复刷屏也不丢失。
func TestRestartContinuesAlertsAndDedup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	cfg := testCfg()

	// --- 第一次“进程”：制造一条 firing 滞后告警 + 一条失败告警 + 一条静默规则 ---
	func() {
		clk := NewMockClock(t0)
		store := NewFileStore(path)
		e := testEngine(t, cfg, clk, store)
		ingest(t, e, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
		if err := e.Silence("p", "op2", CauseStale, 0, "persistent-silence"); err != nil {
			t.Fatal(err)
		}
		ingest(t, e, Event{ID: "s1", Pipeline: "p", Operator: "sink", Kind: KindTaskStarted, TaskID: "T1", OccurredAt: t0})
		ingest(t, e, Event{ID: "f1", Pipeline: "p", Operator: "sink", Kind: KindTaskFinished, TaskID: "T1", OccurredAt: t0.Add(2 * time.Second), Failed: true, ErrorMessage: "boom"})

		clk.Set(t0.Add(40 * time.Second))
		r, _ := e.ObserveTime(context.Background())
		if !hasCause(r.Alerts, CauseStale) {
			t.Fatal("前置：应触发滞后")
		}
		if err := e.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// --- 第二次“进程”：从快照恢复 ---
	clk := NewMockClock(t0.Add(40 * time.Second))
	store := NewFileStore(path)
	e2, err := Open(context.Background(), cfg, clk, store)
	if err != nil {
		t.Fatal(err)
	}
	e2.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = e2.Close(context.Background()) })

	active := e2.ActiveAlerts()
	t.Logf("重启后 firing 告警: %d 条", len(active))
	keys := firingKeys(e2)
	// 预期三条：op 的数据滞后、sink 的数据滞后（10:00:02 后无接入）、
	// 以及 T1 的任务失败。
	for _, want := range []string{
		alertKey("p", "op", CauseStale),
		alertKey("p", "sink", CauseStale),
		failureAlertKey("p", "sink", "T1"),
	} {
		if !keys[want] {
			t.Fatalf("重启后缺失应延续的告警 %s，实际 %v", want, keys)
		}
	}

	// 再次评估：同一轮次不重复产生告警（不刷屏）。
	r, _ := e2.ObserveTime(context.Background())
	if len(r.Alerts) != 0 {
		t.Fatalf("重启后重复评估不应再发告警，got %+v", r.Alerts)
	}

	// 历史事件重放：判重，统计不变。
	rd := ingest(t, e2, Event{ID: "x1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0})
	if !rd.Verdict.Duplicate {
		t.Fatal("重启后重放历史事件必须判重")
	}
	bs := e2.Buckets()
	var records int64
	for _, b := range bs {
		records += b.RecordCount
	}
	// x1 一条记录（finished/started 不是 record），重放不应增加。
	if records != 1 {
		t.Fatalf("重启后重放不得改变统计，records=%d want 1", records)
	}

	// 静默规则延续：让 op2 在静默期内滞后，不应产生对外告警。
	ingest(t, e2, Event{ID: "y1", Pipeline: "p", Operator: "op2", Kind: KindRecord, OccurredAt: t0.Add(40 * time.Second)})
	clk.Set(t0.Add(2 * time.Hour))
	r, _ = e2.ObserveTime(context.Background())
	for _, a := range r.Alerts {
		if a.Pipeline == "p" && a.Operator == "op2" && a.Cause == CauseStale {
			t.Fatal("持久化的静默规则应在重启后继续抑制")
		}
	}
	t.Log("重启恢复验证通过：告警延续、不刷屏、去重与静默均生效")
}
