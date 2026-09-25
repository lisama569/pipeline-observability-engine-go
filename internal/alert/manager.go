package alert

import (
	"strings"
	"sync"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
)

// SilenceEntry mirrors store.Silence, keeping this package free of an import
// cycle. A fingerprint of "*" is the global wildcard silence.
type SilenceEntry struct {
	Fingerprint string
	Comment     string
	StartsAt    time.Time
	EndsAt      time.Time
}

// Manager evaluates conditions against current alerts.
//
// Rules (boundary policy is fixed and unique, see model.Config):
//   - A bad condition with no active alert and no matching silence FIRES once.
//   - While an alert stays active, the same fingerprint re-fires only when
//     now-LastFiredAt is strictly greater than the suppression window; an
//     instant exactly at the boundary is still suppressed.
//   - A good condition RESOLVES the active alert; after resolution the next
//     bad condition fires again immediately (re-deterioration is a new episode).
//   - Silenced fingerprints produce no alert at all; silences use the
//     half-open interval [StartsAt, EndsAt): at == EndsAt the silence is gone.
type Manager struct {
	mu             sync.Mutex
	suppressWindow time.Duration
	alerts         map[string]*model.Alert
	silences       map[string]SilenceEntry
}

func NewManager(suppressWindow time.Duration) *Manager {
	return &Manager{
		suppressWindow: suppressWindow,
		alerts:         map[string]*model.Alert{},
		silences:       map[string]SilenceEntry{},
	}
}

// Fingerprint builds the dedup key for an alert root cause. The root cause is
// exactly (pipeline, operator, reason): e.g. a task_timeout and a task_failure
// of the same operator are different fingerprints.
func Fingerprint(pipelineID, operatorID string, reason model.AlertReason) string {
	return strings.Join([]string{pipelineID, operatorID, string(reason)}, "|")
}

// Apply feeds one evaluated condition. It returns the alert when an observable
// transition happened:
//   - newly fired / re-fired after suppression gap or after resolution
//     (State == active, non-nil),
//   - just resolved (State == resolved),
//
// and nil for unchanged conditions and silenced firings.
func (m *Manager) Apply(cond model.Condition) *model.Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	fp := Fingerprint(cond.PipelineID, cond.OperatorID, cond.Reason)
	now := cond.At
	m.purgeExpiredSilencesLocked(now)

	a, exists := m.alerts[fp]

	if cond.Active {
		if m.isSilencedLocked(fp, now) {
			// Full mute: do not open an alert episode while silenced.
			return nil
		}
		switch {
		case !exists:
			fired := &model.Alert{
				Fingerprint:  fp,
				PipelineID:   cond.PipelineID,
				OperatorID:   cond.OperatorID,
				Reason:       cond.Reason,
				State:        model.AlertActive,
				Message:      cond.Message,
				FirstFiredAt: now,
				LastFiredAt:  now,
				FiredCount:   1,
			}
			m.alerts[fp] = fired
			return fired
		case a.State == model.AlertResolved:
			// Re-deterioration after recovery: a new episode.
			a.State = model.AlertActive
			a.Message = cond.Message
			a.FirstFiredAt = now
			a.LastFiredAt = now
			a.ResolvedAt = nil
			a.SuppressedUntil = time.Time{}
			a.FiredCount++
			return a
		default:
			// Still active: rate-limit repeated notifications.
			if now.Sub(a.LastFiredAt) > m.suppressWindow {
				a.LastFiredAt = now
				a.Message = cond.Message
				a.SuppressedUntil = now.Add(m.suppressWindow)
				a.FiredCount++
				return a
			}
			return nil
		}
	}

	// Condition is healthy.
	if exists && a.State == model.AlertActive {
		a.State = model.AlertResolved
		a.Message = cond.Message
		a.ResolvedAt = &now
		a.SuppressedUntil = time.Time{}
		return a
	}
	return nil
}

// Snapshot returns the internal alert map. Callers must not mutate it while
// the manager is in use (Engine calls this under its own lock in practice).
func (m *Manager) Snapshot() map[string]*model.Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alerts
}

// Restore replaces alert state from a persisted snapshot.
func (m *Manager) Restore(alerts map[string]*model.Alert) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if alerts == nil {
		alerts = map[string]*model.Alert{}
	}
	m.alerts = alerts
}

// AddSilence registers a mute rule. An existing rule for the same
// fingerprint is replaced.
func (m *Manager) AddSilence(fp, comment string, start, end time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.silences[fp] = SilenceEntry{
		Fingerprint: fp,
		Comment:     comment,
		StartsAt:    start,
		EndsAt:      end,
	}
}

// RestoreSilences replaces silence state from a persisted snapshot.
func (m *Manager) RestoreSilences(ss map[string]SilenceEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ss == nil {
		ss = map[string]SilenceEntry{}
	}
	m.silences = ss
}

// Silences returns the current silence rules (including not-yet-started and
// expired ones until PurgeExpiredSilences runs).
func (m *Manager) Silences() map[string]SilenceEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]SilenceEntry, len(m.silences))
	for k, v := range m.silences {
		out[k] = v
	}
	return out
}

// IsSilenced reports whether the fingerprint is muted at time at. Exact
// silence wins; otherwise the "*" wildcard is consulted.
func (m *Manager) IsSilenced(fp string, at time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isSilencedLocked(fp, at)
}

func (m *Manager) isSilencedLocked(fp string, at time.Time) bool {
	if s, ok := m.silences[fp]; ok && s.StartsAt.Compare(at) <= 0 && at.Before(s.EndsAt) {
		return true
	}
	if s, ok := m.silences["*"]; ok && s.StartsAt.Compare(at) <= 0 && at.Before(s.EndsAt) {
		return true
	}
	return false
}

// PurgeExpiredSilences removes rules whose half-open interval has ended
// (EndsAt <= at).
func (m *Manager) PurgeExpiredSilences(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purgeExpiredSilencesLocked(at)
}

func (m *Manager) purgeExpiredSilencesLocked(at time.Time) {
	for k, s := range m.silences {
		if !s.EndsAt.After(at) {
			delete(m.silences, k)
		}
	}
}
