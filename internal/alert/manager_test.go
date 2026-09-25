package alert

import (
	"testing"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
)

func TestManagerLifecycle(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)
	m := NewManager(10 * time.Second)
	cond := model.Condition{PipelineID: "p", OperatorID: "o", Reason: model.ReasonTaskFailure, Active: true, At: t0, Message: "x"}

	if a := m.Apply(cond); a == nil || a.State != model.AlertActive || a.FiredCount != 1 {
		t.Fatalf("first apply must fire")
	}
	t.Logf("fire -> active")

	// Same bad condition within window: nil.
	if a := m.Apply(cond); a != nil {
		t.Fatalf("must suppress within window")
	}
	// Boundary (exactly == window): still suppressed.
	cond.At = t0.Add(10 * time.Second)
	if a := m.Apply(cond); a != nil {
		t.Fatalf("boundary must remain suppressed (strict >)")
	}
	// Past window: renotification.
	cond.At = t0.Add(10*time.Second + time.Nanosecond)
	if a := m.Apply(cond); a == nil || a.FiredCount != 2 {
		t.Fatalf("renotification expected past window")
	}
	t.Logf("renotify past strict boundary -> count 2")

	// Recovery and immediate re-fire.
	good := cond
	good.Active = false
	good.At = t0.Add(20 * time.Second)
	if a := m.Apply(good); a == nil || a.State != model.AlertResolved {
		t.Fatalf("good condition must resolve")
	}
	bad := cond
	bad.At = t0.Add(21 * time.Second)
	if a := m.Apply(bad); a == nil || a.FiredCount != 3 || a.State != model.AlertActive {
		t.Fatalf("re-deterioration must open a new episode, got %+v", a)
	}
	t.Logf("resolve then immediate re-fire -> count 3")
}

func TestManagerSilenceRestored(t *testing.T) {
	t0 := time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)
	m := NewManager(time.Second)
	fp := Fingerprint("p", "o", model.ReasonDataLag)
	m.AddSilence(fp, "m", t0, t0.Add(time.Minute))
	if !m.IsSilenced(fp, t0.Add(30*time.Second)) {
		t.Fatalf("silenced within interval")
	}
	if m.IsSilenced(fp, t0.Add(time.Minute)) {
		t.Fatalf("EndAt is exclusive")
	}
	// Restored alerts keep suppression timing.
	restored := map[string]*model.Alert{
		fp: {Fingerprint: fp, State: model.AlertActive, LastFiredAt: t0, FiredCount: 7},
	}
	m.Restore(restored)
	c := model.Condition{PipelineID: "p", OperatorID: "o", Reason: model.ReasonDataLag, Active: true, At: t0.Add(61 * time.Second)}
	if a := m.Apply(c); a == nil || a.FiredCount != 8 {
		t.Fatalf("restored alert should renotify past window preserving count")
	}
	t.Logf("restored alert preserved FiredCount=7 then renotified 8")
}
