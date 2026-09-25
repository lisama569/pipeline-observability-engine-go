package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st := New(path)

	// Missing file: fresh snapshot, no error.
	snap, err := st.Load()
	if err != nil || snap == nil || len(snap.Stats) != 0 {
		t.Fatalf("missing file must yield empty snapshot, snap=%v err=%v", snap, err)
	}
	t.Logf("missing file loads as empty snapshot")

	now := time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC)
	snap = NewSnapshot(now)
	snap.Stats["p/o"] = model.OperatorStats{PipelineID: "p", OperatorID: "o", Records: 99}
	snap.SeenEvents["e1"] = 123
	if err := st.Save(snap); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := st.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Stats["p/o"].Records != 99 || got.SeenEvents["e1"] != 123 || !got.SavedAt.Equal(now) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	t.Logf("round-trip preserved records=99 seen=123 savedAt=%s", got.SavedAt.Format(time.RFC3339))
}
