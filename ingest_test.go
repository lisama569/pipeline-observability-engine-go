package pipelineobs

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("[engine] "+format, args...)
}

func testEngine(t *testing.T, cfg Config, clk Clock, store Store) *Engine {
	t.Helper()
	e, err := Open(context.Background(), cfg, clk, store)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	e.SetLogger(testLogger{t})
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

func testCfg() Config {
	return Config{
		WindowSize:         time.Minute,
		AllowedLateness:    20 * time.Second,
		StalenessThreshold: 30 * time.Second,
		TaskTimeout:        90 * time.Second,
		TaskStuckThreshold: 45 * time.Second,
		SuppressWindow:     time.Minute,
	}
}

var t0 = time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)

func ingest(t *testing.T, e *Engine, ev Event) IngestResult {
	t.Helper()
	r, err := e.Ingest(context.Background(), ev)
	if err != nil {
		t.Fatalf("ingest %s: %v", ev.ID, err)
	}
	t.Logf("event id=%-8s kind=%-12s occurred=%s => accepted=%t dup=%t expired=%t reason=%-9s %s",
		ev.ID, ev.Kind, ev.OccurredAt.Format("15:04:05"),
		r.Verdict.Accepted, r.Verdict.Duplicate, r.Verdict.Expired, r.Verdict.Reason, r.Verdict.Detail)
	return r
}

// TestConcurrentOutOfOrderAggregation 并发、乱序接入下统计结论确定：
// 同一窗口记录数/字节数精确，重复事件不计数。
//
// 乱序事件全部落在迟到容忍窗口内（最高水位与最低事件时间之差 ≤ 容忍），
// 因此真正并发乱序到达也不会误判过期；超窗语义在另一个用例专门验证。
func TestConcurrentOutOfOrderAggregation(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	// 乱序：5s, 20s, 15s, 10s, 25s（跨度 20s = AllowedLateness，边界安全）。
	offs := []time.Duration{5 * time.Second, 20 * time.Second, 15 * time.Second, 10 * time.Second, 25 * time.Second}
	var wg sync.WaitGroup
	for i, off := range offs {
		wg.Add(1)
		go func(i int, off time.Duration) {
			defer wg.Done()
			ingest(t, e, Event{
				ID: fmt.Sprintf("r%d", i), Pipeline: "p", Operator: "op",
				Kind: KindRecord, OccurredAt: t0.Add(off), Bytes: int64(10 * (i + 1)),
			})
		}(i, off)
	}
	wg.Wait()

	// 重复投递（同 ID、同内容）并发两次：只能都是 duplicate，计数不变。
	for k := 0; k < 2; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := ingest(t, e, Event{ID: "r0", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(5 * time.Second), Bytes: 10})
			if !r.Verdict.Duplicate || r.Verdict.Accepted {
				t.Errorf("重复事件应判 duplicate 且不计数，got %+v", r.Verdict)
			}
		}()
	}
	wg.Wait()

	bs := e.Buckets()
	if len(bs) != 1 {
		t.Fatalf("应只有 1 个窗口，got %d", len(bs))
	}
	b := bs[0]
	if b.RecordCount != 5 {
		t.Errorf("记录数应为 5（重复不计），got %d", b.RecordCount)
	}
	// 10+20+30+40+50（字节为 10*(i+1)，与到达顺序无关）。
	if b.ByteCount != 150 {
		t.Errorf("字节数应为 150，got %d", b.ByteCount)
	}
	t.Logf("窗口统计: records=%d bytes=%d latencyAvgMS=%.2f", b.RecordCount, b.ByteCount, b.LatencyAvgMS)
}

// TestLateWithinToleranceAndExpired 迟到容忍内并入旧窗口，超出窗口标记过期且可解释。
func TestLateWithinToleranceAndExpired(t *testing.T) {
	clk := NewMockClock(t0)
	e := testEngine(t, testCfg(), clk, nil)

	ingest(t, e, Event{ID: "a1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(10 * time.Second)})
	// 水位推进到 10:01:10；容忍下界 = 10:00:50。
	clk.Set(t0.Add(70 * time.Second))
	ingest(t, e, Event{ID: "a2", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(70 * time.Second)})

	// 边界点：事件时间恰好等于容忍下界 10:00:50 => 在容忍内（>= 口径），并入。
	rBoundary := ingest(t, e, Event{ID: "late-edge", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(50 * time.Second)})
	if !rBoundary.Verdict.Accepted {
		t.Errorf("边界事件（恰好下界）应并入，got reason=%s", rBoundary.Verdict.Reason)
	}

	// 容忍内迟到：10:00:55。
	rLate := ingest(t, e, Event{ID: "late", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(55 * time.Second)})
	if !rLate.Verdict.Accepted || rLate.Verdict.Reason != ReasonNone {
		t.Errorf("容忍内迟到应并入，got %+v", rLate.Verdict)
	}

	// 超出窗口：10:00:49 => 过期，明确标记。
	rExp := ingest(t, e, Event{ID: "old", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(49 * time.Second)})
	if rExp.Verdict.Accepted || !rExp.Verdict.Expired || rExp.Verdict.Reason != ReasonExpired {
		t.Errorf("超窗事件应判 expired 且不计数，got %+v", rExp.Verdict)
	}

	// 过期事件重复投递：结论保持 expired（不被重新计数，也不变成普通 duplicate 计数）。
	rExpDup := ingest(t, e, Event{ID: "old", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(49 * time.Second)})
	if !rExpDup.Verdict.Duplicate {
		t.Errorf("过期事件重放应判 duplicate 并维持过期结论，got %+v", rExpDup.Verdict)
	}

	bs := e.Buckets()
	var first *BucketView
	for i := range bs {
		if bs[i].WindowStart.Equal(t0) {
			first = &bs[i]
		}
	}
	if first == nil {
		t.Fatal("找不到 10:00 窗口")
	}
	// 该窗口：a1 + late-edge + late = 3 条；过期 old 不计。
	if first.RecordCount != 3 {
		t.Errorf("旧窗口应有 3 条（含 2 条迟到/边界），got %d", first.RecordCount)
	}
	if !first.Closed {
		t.Errorf("水位已越过 10:01:00，该窗口应已关闭")
	}
}

// TestDeterministicUnderRepeatedRun 相同事件序列重复执行（新引擎），
// 统计结论必须逐字节一致（可复现）。
func TestDeterministicUnderRepeatedRun(t *testing.T) {
	run := func() []BucketView {
		clk := NewMockClock(t0)
		e := testEngine(t, testCfg(), clk, nil)
		seq := []Event{
			{ID: "1", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(2 * time.Second), Bytes: 1},
			{ID: "2", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(8 * time.Second), Bytes: 2},
			{ID: "3", Pipeline: "p", Operator: "op", Kind: KindRecord, OccurredAt: t0.Add(3 * time.Second), Bytes: 3},
		}
		clk.Set(t0.Add(20 * time.Second))
		for _, ev := range seq {
			ingest(t, e, ev)
		}
		return e.Buckets()
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("两次运行窗口数不同 %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("窗口结论不一致:\n%+v\n%+v", a[i], b[i])
		}
	}
}
