package engine

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/alert"
	"github.com/lisama569/pipeline-observability-engine-go/internal/clock"
	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
	"github.com/lisama569/pipeline-observability-engine-go/internal/store"
)

func testConfig() model.Config {
	return model.Config{
		LatenessTolerance:   10 * time.Second,
		DataLagThreshold:    30 * time.Second,
		TaskTimeout:         60 * time.Second,
		AlertSuppressWindow: 100 * time.Second,
		EvaluationInterval:  time.Second,
	}
}

func newTestEngine(t *testing.T, fc *clock.FakeClock) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st := store.New(path)
	e, err := New(testConfig(), fc, st)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, path
}

func ev(id, pipe, op, run string, typ model.EventType, et time.Time, rec int64, errMsg string) model.Event {
	return model.Event{
		ID: id, PipelineID: pipe, OperatorID: op, RunID: run,
		Type: typ, EventTime: et, Records: rec, Error: errMsg,
	}
}

func findAlert(m map[string]*model.Alert, reason model.AlertReason) *model.Alert {
	for _, a := range m {
		if a.Reason == reason {
			return a
		}
	}
	return nil
}

// TestBasicAggregationAndDedup: concurrent/out-of-order delivery must produce
// the same totals, and the same event ID must be counted exactly once.
func TestBasicAggregationAndDedup(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	// A watermark first, then events older/newer in deliberately odd order.
	fc.Advance(time.Second)
	r := e.Ingest(ev("w1", "p", "o", "", model.EventWatermark, t0.Add(5*time.Second), 0, ""))
	if !r.Accepted {
		t.Fatalf("watermark accepted: %+v", r)
	}
	t.Logf("watermark -> accepted, watermark=%s reason=%q", r.Watermark.Format(time.RFC3339), r.Reason)

	// On time, then late-but-within-window (5s old, tolerance 10s), out of order.
	fc.Advance(time.Second)
	r = e.Ingest(ev("r1", "p", "o", "", model.EventRecordWritten, t0.Add(6*time.Second), 10, ""))
	t.Logf("r1 -> accepted=%v expired=%v dup=%v reason=%q", r.Accepted, r.Expired, r.Duplicate, r.Reason)
	if !r.Accepted {
		t.Fatalf("r1 should be accepted: %+v", r)
	}

	fc.Advance(time.Second)
	r = e.Ingest(ev("r0-late", "p", "o", "", model.EventRecordWritten, t0.Add(0*time.Second), 3, ""))
	if !r.Accepted {
		t.Fatalf("event 5s late (watermark 5s, tolerance 10s) must be accepted: %+v", r)
	}
	t.Logf("late-within-window -> accepted; reason=%q watermark=%s", r.Reason, r.Watermark.Format(time.RFC3339))

	// Boundary: exactly at watermark-tolerance (t0-5s) is accepted.
	r = e.Ingest(ev("r-boundary", "p", "o", "", model.EventRecordWritten, t0.Add(-5*time.Second), 1, ""))
	if !r.Accepted {
		t.Fatalf("boundary event must be accepted (closed boundary): %+v", r)
	}
	t.Logf("boundary-late -> accepted")

	// Beyond the window -> expired, explicitly flagged, not silently dropped.
	r = e.Ingest(ev("r-old", "p", "o", "", model.EventRecordWritten, t0.Add(-6*time.Second), 99, ""))
	if !r.Expired {
		t.Fatalf("event older than watermark-tolerance must be expired: %+v", r)
	}
	t.Logf("expired event -> expired=true reason=%q", r.Reason)

	// Duplicate delivery of the expired event: still not counted again.
	r = e.Ingest(ev("r-old", "p", "o", "", model.EventRecordWritten, t0.Add(-6*time.Second), 99, ""))
	if !r.Duplicate {
		t.Fatalf("redelivered expired event must be duplicate: %+v", r)
	}
	// Duplicate of an accepted event.
	r = e.Ingest(ev("r1", "p", "o", "", model.EventRecordWritten, t0.Add(6*time.Second), 10, ""))
	if !r.Duplicate {
		t.Fatalf("redelivered event must be duplicate: %+v", r)
	}

	s := e.Stats()["p/o"]
	if s.Records != 14 { // 10 + 3 + 1, expired 99 excluded
		t.Fatalf("records = %d, want 14", s.Records)
	}
	if s.ExpiredEvents != 1 {
		t.Fatalf("expired = %d, want 1", s.ExpiredEvents)
	}
	if s.DuplicateEvents != 2 {
		t.Fatalf("duplicates = %d, want 2", s.DuplicateEvents)
	}
	t.Logf("FINAL stats: records=%d expired=%d duplicates=%d latestEventTime=%s",
		s.Records, s.ExpiredEvents, s.DuplicateEvents, s.LatestEventTime.Format(time.RFC3339))
}

// TestConcurrentIngestConsistency fans events in from many goroutines, with
// deliberate duplicate redelivery. Final totals must be identical to the
// serial expectation regardless of scheduling.
func TestConcurrentIngestConsistency(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	const writers = 16
	const perWriter = 200

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					id := fmt.Sprintf("w%d-e%d", w, i)
					et := t0.Add(time.Duration(w*perWriter+i) * time.Second)
					_ = e.Ingest(ev(id, "p", "c", "", model.EventRecordWritten, et, 2, ""))
					// Every 25th event is redelivered by somebody else.
					if i%25 == 0 {
						_ = e.Ingest(ev(id, "p", "c", "", model.EventRecordWritten, et, 2, ""))
					}
				}
			}(w)
		}
		wg.Wait()
		close(done)
	}()
	<-done

	s := e.Stats()["p/c"]
	wantRecords := int64(writers * perWriter * 2)
	if s.Records != wantRecords {
		t.Fatalf("concurrent records = %d, want %d", s.Records, wantRecords)
	}
	wantDups := int64(writers * (perWriter / 25))
	if s.DuplicateEvents != wantDups {
		t.Fatalf("concurrent duplicates = %d, want %d", s.DuplicateEvents, wantDups)
	}
	t.Logf("concurrent ingest stable: writers=%d uniqueEvents=%d records=%d duplicates=%d",
		writers, writers*perWriter, s.Records, s.DuplicateEvents)
}

// TestOutOfOrderRunLifecycle: failure arrives before started, success even
// earlier; conclusions must follow event time, not arrival order.
func TestOutOfOrderRunLifecycle(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	run := "run-1"
	// Deliver failure first (newest event time), then started (oldest).
	_ = e.Ingest(ev("f1", "p", "o", run, model.EventTaskFailed, t0.Add(10*time.Second), 0, "boom"))
	_ = e.Ingest(ev("s1", "p", "o", run, model.EventTaskStarted, t0, 0, ""))

	changed := e.Evaluate()
	if len(changed) != 1 || changed[0].Reason != model.ReasonTaskFailure {
		t.Fatalf("want one task_failure alert, got %+v", changed)
	}
	t.Logf("out-of-order failure after evaluation: fired reason=%s msg=%s", changed[0].Reason, changed[0].Message)

	// Late success with an even later event time overrides failure; the
	// failure alert must resolve on the next evaluation.
	_ = e.Ingest(ev("ok1", "p", "o", run, model.EventTaskSucceeded, t0.Add(20*time.Second), 0, ""))
	changed = e.Evaluate()
	var resolved *model.Alert
	for _, a := range changed {
		if a.Reason == model.ReasonTaskFailure && a.State == model.AlertResolved {
			resolved = a
		}
	}
	if resolved == nil {
		t.Fatalf("task_failure alert should resolve after late success; changed=%+v", changed)
	}
	t.Logf("late success resolved failure at %s", resolved.ResolvedAt.Format(time.RFC3339Nano))
}

// TestTimeoutAndLagThresholds checks the three distinct causes and the
// fixed boundary policy: elapsed == threshold is still healthy; strictly
// greater trips the condition.
func TestTimeoutAndLagThresholds(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 13, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	_ = e.Ingest(ev("start", "p", "o", "run-x", model.EventTaskStarted, t0, 0, ""))
	_ = e.Ingest(ev("data1", "p", "o", "", model.EventRecordWritten, t0, 5, ""))

	// age == lag threshold (30s), elapsed == 30s < timeout: healthy.
	fc.Set(t0.Add(30 * time.Second))
	if a := e.Evaluate(); len(a) != 0 {
		t.Fatalf("lag boundary instant must be healthy, got %+v", a)
	}
	t.Logf("at lag age==30s (closed): healthy")

	// One nanosecond past the lag threshold: only data_lag trips.
	fc.Set(t0.Add(30*time.Second + time.Nanosecond))
	got := e.Evaluate()
	if len(got) != 1 || got[0].Reason != model.ReasonDataLag {
		t.Fatalf("want only data_lag, got %+v", got)
	}
	t.Logf("data_lag fired: %s", got[0].Message)

	// elapsed == timeout exactly: timeout must NOT fire yet; lag is rate-limited.
	fc.Set(t0.Add(60 * time.Second))
	if a := e.Evaluate(); len(a) != 0 {
		t.Fatalf("timeout boundary must be healthy and lag suppressed, got %+v", a)
	}
	t.Logf("at elapsed==60s (==timeout): timeout not fired; lag still suppressed")

	// One nanosecond beyond timeout: timeout fires as a distinct root cause.
	fc.Set(t0.Add(60*time.Second + time.Nanosecond))
	got = e.Evaluate()
	if len(got) != 1 || got[0].Reason != model.ReasonTaskTimeout {
		t.Fatalf("want only task_timeout newly fired, got %+v", got)
	}
	t.Logf("task_timeout fired: %s", got[0].Message)

	// Fresh data clears data_lag only; the timed-out run remains active.
	_ = e.Ingest(ev("data2", "p", "o", "", model.EventRecordWritten, fc.Now(), 7, ""))
	got = e.Evaluate()
	var lagResolved bool
	for _, a := range got {
		if a.Reason == model.ReasonDataLag && a.State == model.AlertResolved {
			lagResolved = true
		}
		if a.State == model.AlertActive {
			t.Fatalf("no new firing expected after fresh data, got %+v", a)
		}
	}
	if !lagResolved {
		t.Fatalf("data_lag must resolve after fresh data, got %+v", got)
	}
	a := findAlert(e.Alerts(), model.ReasonTaskTimeout)
	if a == nil || a.State != model.AlertActive {
		t.Fatalf("timeout alert must stay active until run finishes")
	}
	t.Logf("fresh data resolved data_lag; task_timeout stays active")
}

// TestClockRollbackStability: stepping the wall clock backwards must not
// fabricate or erase conditions; the same evaluation stays reproducible.
func TestClockRollbackStability(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	_ = e.Ingest(ev("d", "p", "o", "", model.EventRecordWritten, t0, 1, ""))
	fc.Advance(120 * time.Second)
	lag1 := e.Evaluate()
	if len(lag1) != 1 || lag1[0].Reason != model.ReasonDataLag {
		t.Fatalf("expected data_lag after 120s, got %+v", lag1)
	}

	// Roll back the clock: the lag must not resolve from rollback alone,
	// and no duplicate alert should fire.
	fc.Set(t0.Add(10 * time.Second))
	churn := e.Evaluate()
	for _, a := range churn {
		if a.Reason == model.ReasonDataLag && a.State == model.AlertResolved {
			t.Fatalf("clock rollback must not resolve a real lag condition")
		}
	}
	a := findAlert(e.Alerts(), model.ReasonDataLag)
	if a == nil || a.State != model.AlertActive || a.FiredCount != 1 {
		t.Fatalf("single active data_lag firing expected, got %+v", a)
	}
	t.Logf("rollback stable: alert stays active firedCount=%d", a.FiredCount)

	// Forward again: still the same single episode (suppression window 100s).
	fc.Set(t0.Add(120 * time.Second))
	if churn := e.Evaluate(); len(churn) != 0 {
		t.Fatalf("within suppression window nothing should re-fire, got %+v", churn)
	}
}

// TestAlertDedupSuppressRefire covers the whole alert lifecycle:
// one root cause -> one alert in-window; re-notification only after the
// suppression gap; resolution; immediate re-fire on re-deterioration.
func TestAlertDedupSuppressRefire(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 15, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	_ = e.Ingest(ev("d", "p", "o", "", model.EventRecordWritten, t0, 1, ""))
	fc.Advance(31 * time.Second)
	fired := e.Evaluate()
	if len(fired) != 1 || fired[0].State != model.AlertActive {
		t.Fatalf("first firing expected, got %+v", fired)
	}
	t.Logf("fired: fp=%s at %s", fired[0].Fingerprint, fired[0].FirstFiredAt.Format(time.RFC3339))

	// Repeated evaluations inside the window: no spam.
	for i := 0; i < 5; i++ {
		fc.Advance(10 * time.Second)
		if ch := e.Evaluate(); len(ch) != 0 {
			t.Fatalf("within suppress window no new alert, got %+v", ch)
		}
	}
	if a := findAlert(e.Alerts(), model.ReasonDataLag); a.FiredCount != 1 {
		t.Fatalf("FiredCount = %d, want 1", a.FiredCount)
	}
	t.Logf("suppressed 5 repeated evaluations; still one effective alert")

	// Past the suppression gap (100s after LastFiredAt): one renotification.
	fc.Advance(100 * time.Second) // t0+181 => 150s after last firing? 31+50=81; +100=181, gap 100s
	ch := e.Evaluate()
	if len(ch) != 1 || ch[0].FiredCount != 2 {
		t.Fatalf("expected one re-notification (count 2), got %+v", ch)
	}
	t.Logf("re-notified once after suppression gap; firedCount=2")

	// Recovery: fresh data resolves.
	_ = e.Ingest(ev("d2", "p", "o", "", model.EventRecordWritten, fc.Now(), 1, ""))
	ch = e.Evaluate()
	if len(ch) != 1 || ch[0].State != model.AlertResolved {
		t.Fatalf("expected resolution, got %+v", ch)
	}

	// Re-deterioration immediately opens a new episode (new firing).
	fc.Advance(31 * time.Second)
	ch = e.Evaluate()
	if len(ch) != 1 || ch[0].State != model.AlertActive || ch[0].FiredCount != 3 {
		t.Fatalf("expected re-fire after recovery, got %+v", ch)
	}
	t.Logf("re-deterioration after recovery re-fired; firedCount=3")
}

// TestSilence: an active silence mutes new alerts; once the half-open
// silence interval ends the next evaluation fires; wildcard works too.
func TestSilence(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 16, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, _ := newTestEngine(t, fc)

	_ = e.Ingest(ev("d", "p", "o", "", model.EventRecordWritten, t0, 1, ""))
	fp := alert.Fingerprint("p", "o", model.ReasonDataLag)
	e.AddSilence(fp, "maintenance", t0, t0.Add(10*time.Minute))
	fc.Advance(31 * time.Second)
	if ch := e.Evaluate(); len(ch) != 0 {
		t.Fatalf("silenced fingerprint must not alert, got %+v", ch)
	}
	t.Logf("silence muted data_lag during maintenance window")

	// Silence interval ends (boundary EndAt exclusive).
	fc.Set(t0.Add(10 * time.Minute))
	ch := e.Evaluate()
	if len(ch) != 1 || ch[0].Reason != model.ReasonDataLag {
		t.Fatalf("at silence EndAt it is expired; alert should fire, got %+v", ch)
	}
	t.Logf("silence expired at EndAt; alert fired")

	// Wildcard silence mutes any new root cause on another operator.
	e.AddSilence("*", "global", fc.Now(), fc.Now().Add(time.Minute))
	_ = e.Ingest(ev("d2", "p", "o2", "", model.EventRecordWritten, fc.Now(), 1, ""))
	fc.Advance(31 * time.Second)
	ch = e.Evaluate()
	for _, a := range ch {
		if a.Reason == model.ReasonDataLag && a.OperatorID == "o2" {
			t.Fatalf("wildcard silence must mute o2 data_lag")
		}
	}
	t.Logf("wildcard '*' silence muted a different operator")
}

// TestRestartRecovery simulates a process restart by building a second engine
// over the same snapshot file. Aggregates, dedup set, watermark, active
// alerts, suppression timing and silences must all continue:
// no duplicate counts, no lost/duplicate alert episodes.
func TestRestartRecovery(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 17, 0, 0, 0, time.UTC)
	fc := clock.NewFake(t0)
	e, path := newTestEngine(t, fc)

	_ = e.Ingest(ev("d1", "p", "o", "", model.EventRecordWritten, t0, 42, ""))
	_ = e.Ingest(ev("w1", "p", "o", "", model.EventWatermark, t0.Add(2*time.Second), 0, ""))
	e.AddSilence("*", "deploy window", t0.Add(2*time.Minute), t0.Add(20*time.Minute))

	fc.Advance(31 * time.Second)
	ch := e.Evaluate()
	// Active data_lag but the wildcard silence starts in the future, so it
	// fires once now.
	if len(ch) != 1 || ch[0].Reason != model.ReasonDataLag {
		t.Fatalf("expected one data_lag firing pre-restart, got %+v", ch)
	}
	first := ch[0]
	t.Logf("pre-restart fired %s at %s (count=%d)", first.Fingerprint,
		first.FirstFiredAt.Format(time.RFC3339), first.FiredCount)

	if err := e.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	// --- "restart": new engine, same file, wall clock jumps forward 40s. ---
	fc.Set(t0.Add(71 * time.Second))
	e2, err := New(testConfig(), fc, store.New(path))
	if err != nil {
		t.Fatalf("restart New: %v", err)
	}
	t.Cleanup(func() { _ = e2.Close() })

	// 1) Aggregates continued.
	if got := e2.Stats()["p/o"].Records; got != 42 {
		t.Fatalf("records after restart = %d, want 42", got)
	}
	// 2) Dedup set continued: redelivery is duplicate, never double-counted.
	if r := e2.Ingest(ev("d1", "p", "o", "", model.EventRecordWritten, t0, 42, "")); !r.Duplicate {
		t.Fatalf("redelivered pre-restart event must be duplicate, got %+v", r)
	}
	t.Logf("after restart: records still 42; old event redelivery classified duplicate")

	// 3) Expired semantics continue from the restored watermark: an event
	//    older than watermark-tolerance is expired.
	r := e2.Ingest(ev("old-after-restart", "p", "o", "",
		model.EventRecordWritten, t0.Add(-9*time.Second), 5, ""))
	if !r.Expired {
		t.Fatalf("expected expired relative to restored watermark, got %+v", r)
	}
	t.Logf("after restart: late event %s", r.Reason)

	// 4) Alert episode continues: evaluation produces no new firing while
	//    the active episode is inside the suppression window (LastFiredAt
	//    at t0+31; now t0+71 => 40s < 100s) and the wildcard silence is now
	//    active anyway.
	ch = e2.Evaluate()
	for _, a := range ch {
		if a.Reason == model.ReasonDataLag && a.State == model.AlertActive {
			t.Fatalf("active episode must not spam after restart, got %+v", a)
		}
	}
	a := findAlert(e2.Alerts(), model.ReasonDataLag)
	if a == nil || a.State != model.AlertActive || a.FiredCount != 1 ||
		!a.FirstFiredAt.Equal(first.FirstFiredAt) {
		t.Fatalf("alert episode must survive restart unchanged, got %+v", a)
	}
	t.Logf("after restart: same alert episode active count=%d firstFired=%s (no spam)",
		a.FiredCount, a.FirstFiredAt.Format(time.RFC3339))

	// 5) Silence survived: even after the suppression gap it stays muted.
	fc.Set(t0.Add(200 * time.Second))
	ch = e2.Evaluate()
	if len(ch) != 0 {
		t.Fatalf("surviving wildcard silence should still mute, got %+v", ch)
	}
	t.Logf("after restart: surviving wildcard silence still mutes at %s", fc.Now().Format(time.RFC3339))

	// 6) After the silence ends and a re-notification is due, it fires as a
	//    continuation (count 2), proving suppression + silence both continued.
	fc.Set(t0.Add(21 * time.Minute))
	ch = e2.Evaluate()
	if len(ch) != 1 || ch[0].FiredCount != 2 {
		t.Fatalf("expected one continuation firing (count 2) after silence end, got %+v", ch)
	}
	t.Logf("silence ended; one continuation firing count=2 at %s", ch[0].LastFiredAt.Format(time.RFC3339))
}
