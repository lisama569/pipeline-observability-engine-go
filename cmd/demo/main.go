// Command demo runs a fully local, dependency-free demonstration of the
// pipeline observability engine: concurrent + out-of-order event ingest,
// late/expired handling, distinct failure causes, alert dedup/silence/
// recovery and a simulated restart from the persisted snapshot.
//
// Run with:  go run ./cmd/demo
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/clock"
	"github.com/lisama569/pipeline-observability-engine-go/internal/engine"
	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
	"github.com/lisama569/pipeline-observability-engine-go/internal/store"
)

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func main() {
	statePath := filepath.Join(os.TempDir(), "pipeline-observability-demo-state.json")
	// The demo is a self-contained scripted scenario; start from a clean
	// snapshot each time. The restart-recovery semantics it demonstrates are
	// covered against real persistence by the unit tests.
	_ = os.Remove(statePath)

	cfg := model.Config{
		LatenessTolerance:   5 * time.Second,
		DataLagThreshold:    20 * time.Second,
		TaskTimeout:         30 * time.Second,
		AlertSuppressWindow: time.Minute,
		EvaluationInterval:  time.Second,
	}

	t0 := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)

	fmt.Println("=== phase 1: ingest concurrent / out-of-order events ===")
	run(t0, fc, cfg, statePath)

	fmt.Println()
	fmt.Println("=== phase 2: simulated process restart (same snapshot file) ===")
	restart(t0, fc, cfg, statePath)
}

func run(t0 time.Time, fc *clock.FakeClock, cfg model.Config, statePath string) {
	st := store.New(statePath)
	e, err := engine.New(cfg, fc, st)
	must(err)

	// Watermark announced early; later events arrive out of order.
	show(e.Ingest(model.Event{ID: "wm", PipelineID: "p1", OperatorID: "source",
		Type: model.EventWatermark, EventTime: t0.Add(8 * time.Second)}), "watermark @ +8s")

	events := []model.Event{
		{ID: "st", PipelineID: "p1", OperatorID: "transform", RunID: "r1",
			Type: model.EventTaskStarted, EventTime: t0},
		{ID: "ok", PipelineID: "p1", OperatorID: "source",
			Type: model.EventRecordWritten, EventTime: t0.Add(9 * time.Second), Records: 100},
		{ID: "late-ok", PipelineID: "p1", OperatorID: "source",
			Type: model.EventRecordWritten, EventTime: t0.Add(4 * time.Second), Records: 5},
		{ID: "too-old", PipelineID: "p1", OperatorID: "source",
			Type: model.EventRecordWritten, EventTime: t0.Add(2 * time.Second), Records: 999},
	}
	var wg sync.WaitGroup
	for _, ev := range events {
		wg.Add(1)
		go func(ev model.Event) { defer wg.Done(); show(e.Ingest(ev), ev.ID) }(ev)
	}
	wg.Wait()

	// Redelivery: exactly-once accounting.
	show(e.Ingest(model.Event{ID: "ok", PipelineID: "p1", OperatorID: "source",
		Type: model.EventRecordWritten, EventTime: t0.Add(9 * time.Second), Records: 100}), "ok (redelivered)")

	fc.Set(t0.Add(31 * time.Second)) // run r1 timed out, source data lagging
	for _, a := range emit(e) {
		fmt.Printf("  %s %-12s %s/%s %s\n", tag(a), a.Reason, a.PipelineID, a.OperatorID, a.Message)
	}

	fc.Set(t0.Add(40 * time.Second))
	show(e.Ingest(model.Event{ID: "fail", PipelineID: "p1", OperatorID: "transform", RunID: "r1",
		Type: model.EventTaskFailed, EventTime: t0.Add(35 * time.Second),
		Error: "non-zero exit 42"}), "late failure of r1")
	for _, a := range e.Evaluate() {
		fmt.Printf("  ALERT %-12s %s/%s %s\n", a.Reason, a.PipelineID, a.OperatorID, a.Message)
	}

	must(e.Save())
	fmt.Printf("state saved to %s\n", statePath)
	must(e.Close())
}

func restart(t0 time.Time, fc *clock.FakeClock, cfg model.Config, statePath string) {
	fc.Set(t0.Add(90 * time.Second))
	e, err := engine.New(cfg, fc, store.New(statePath))
	must(err)
	defer e.Close()

	// Dedup set survived restart.
	show(e.Ingest(model.Event{ID: "ok", PipelineID: "p1", OperatorID: "source",
		Type: model.EventRecordWritten, EventTime: t0.Add(9 * time.Second), Records: 100}), "ok (after restart)")

	// Active alert episodes survived: no duplicate spam.
	changed := e.Evaluate()
	fmt.Printf("evaluations after restart: %d transition(s) (active episodes continue, no spam)\n", len(changed))

	// Recovery: fresh data + success resolves everything.
	show(e.Ingest(model.Event{ID: "fresh", PipelineID: "p1", OperatorID: "source",
		Type: model.EventRecordWritten, EventTime: fc.Now(), Records: 1}), "fresh data")
	show(e.Ingest(model.Event{ID: "r2-start", PipelineID: "p1", OperatorID: "transform", RunID: "r2",
		Type: model.EventTaskStarted, EventTime: fc.Now()}), "new run r2")
	for _, a := range emit(e) {
		fmt.Printf("  %s %-12s %s/%s %s\n", tag(a), a.Reason, a.PipelineID, a.OperatorID, a.Message)
	}
	must(e.Save())
}

func show(r model.IngestResult, label string) {
	switch {
	case r.Accepted:
		fmt.Printf("  ingest %-22s accepted\n", label)
	case r.Duplicate:
		fmt.Printf("  ingest %-22s duplicate (%s)\n", label, r.Reason)
	case r.Expired:
		fmt.Printf("  ingest %-22s EXPIRED  (%s)\n", label, r.Reason)
	default:
		fmt.Printf("  ingest %-22s rejected (%s)\n", label, r.Reason)
	}
}

func emit(e *engine.Engine) []*model.Alert { return e.Evaluate() }

func tag(a *model.Alert) string {
	if a.State == model.AlertResolved {
		return "RECOVER"
	}
	return "ALERT  "
}
