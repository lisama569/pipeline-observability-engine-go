package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
)

// Snapshot is the full persisted state of one engine instance. It is the
// single source of truth across restarts: operator aggregates, the dedup set
// of already seen event IDs, per-operator watermarks, active/resolved alerts
// and active silences. Writes are atomic (temp file + fsync + rename), so a
// crash leaves either the previous or the new snapshot, never a torn one.
type Snapshot struct {
	Version    int                            `json:"version"`
	SavedAt    time.Time                      `json:"saved_at"`
	Stats      map[string]model.OperatorStats `json:"stats"`
	SeenEvents map[string]int64               `json:"seen_events"`
	Watermarks map[string]time.Time           `json:"watermarks"`
	Alerts     map[string]*model.Alert        `json:"alerts"`
	Silences   map[string]Silence             `json:"silences"`
}

// Silence is a manual mute rule for a fingerprint ("*" matches everything).
type Silence struct {
	Fingerprint string    `json:"fingerprint"`
	Comment     string    `json:"comment"`
	StartsAt    time.Time `json:"starts_at"`
	EndsAt      time.Time `json:"ends_at"`
}

const currentVersion = 1

// Store persists engine snapshots locally as JSON. No network is involved.
type Store struct {
	path string
}

func New(path string) *Store { return &Store{path: path} }

// NewSnapshot returns an initialized, empty snapshot.
func NewSnapshot(now time.Time) *Snapshot {
	return &Snapshot{
		Version:    currentVersion,
		SavedAt:    now,
		Stats:      map[string]model.OperatorStats{},
		SeenEvents: map[string]int64{},
		Watermarks: map[string]time.Time{},
		Alerts:     map[string]*model.Alert{},
		Silences:   map[string]Silence{},
	}
}

// Save atomically writes the snapshot.
func (s *Store) Save(snap *Snapshot) error {
	if snap == nil {
		return errors.New("nil snapshot")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	snap.Version = currentVersion
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".snap-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

// Load reads the snapshot; a missing file yields a fresh empty snapshot with
// ok=true, so first boot and restart share one code path.
func (s *Store) Load() (*Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return NewSnapshot(time.Time{}), nil
		}
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	if snap.Version != currentVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", snap.Version)
	}
	if snap.Stats == nil {
		snap.Stats = map[string]model.OperatorStats{}
	}
	if snap.SeenEvents == nil {
		snap.SeenEvents = map[string]int64{}
	}
	if snap.Watermarks == nil {
		snap.Watermarks = map[string]time.Time{}
	}
	if snap.Alerts == nil {
		snap.Alerts = map[string]*model.Alert{}
	}
	if snap.Silences == nil {
		snap.Silences = map[string]Silence{}
	}
	return &snap, nil
}

func (s *Store) Path() string { return s.path }
