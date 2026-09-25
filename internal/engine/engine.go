package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/alert"
	"github.com/lisama569/pipeline-observability-engine-go/internal/clock"
	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
	"github.com/lisama569/pipeline-observability-engine-go/internal/store"
)

// Engine is the in-process pipeline observability component. All public
// methods are safe for concurrent use: a single mutex makes the combined
// decision (dedup + windowing + aggregation + alert transition) atomic, which
// is what keeps concurrent/out-of-order delivery from producing inconsistent
// counts or double alerts.
type Engine struct {
	mu        sync.Mutex
	cfg       model.Config
	clk       clock.Clock
	st        *store.Store
	stats     map[string]model.OperatorStats
	seen      map[string]int64     // event ID -> event time unix nano
	watermark map[string]time.Time // per operator: announced watermark
	alerts    *alert.Manager
	savedAt   time.Time
	// monoNow is the monotonic processing-time reference. It never moves
	// backwards within a process even if the injected wall clock does, so an
	// NTP-style rollback cannot erase elapsed time or resolve real lag.
	monoNow time.Time
}

// New constructs an engine and, when st is non-nil, restorts its full state
// from the local snapshot file (missing file = fresh start). Persisted active
// alerts keep their LastFiredAt, so suppression survives a restart; active
// silences survive as well.
func New(cfg model.Config, clk clock.Clock, st *store.Store) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clk == nil {
		clk = clock.RealClock{}
	}
	e := &Engine{
		cfg:       cfg,
		clk:       clk,
		st:        st,
		stats:     map[string]model.OperatorStats{},
		seen:      map[string]int64{},
		watermark: map[string]time.Time{},
		alerts:    alert.NewManager(cfg.AlertSuppressWindow),
	}
	if st != nil {
		snap, err := st.Load()
		if err != nil {
			return nil, fmt.Errorf("restore state: %w", err)
		}
		e.stats = snap.Stats
		e.seen = snap.SeenEvents
		e.watermark = snap.Watermarks
		e.savedAt = snap.SavedAt
		e.monoNow = snap.SavedAt // processing time continues past the last save
		e.alerts.Restore(snap.Alerts)
		sil := make(map[string]alert.SilenceEntry, len(snap.Silences))
		for k, s := range snap.Silences {
			sil[k] = alert.SilenceEntry{
				Fingerprint: s.Fingerprint,
				Comment:     s.Comment,
				StartsAt:    s.StartsAt,
				EndsAt:      s.EndsAt,
			}
		}
		e.alerts.RestoreSilences(sil)
	}
	return e, nil
}

// Ingest accepts one event. The result explicitly classifies it as accepted,
// duplicate, expired or rejected (malformed). It never silently drops data.
func (e *Engine) Ingest(ev model.Event) model.IngestResult {
	if err := ev.Validate(); err != nil {
		return model.IngestResult{Reason: "rejected: " + err.Error()}
	}
	if isLifecycle(ev.Type) && ev.RunID == "" {
		return model.IngestResult{Reason: "rejected: run_id is required for task lifecycle events"}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.nowLocked()
	key := ev.Key()

	// 1) Exactly-once accounting: the first delivery of an event ID is the
	//    authority, regardless of how late it was. Redelivery never changes
	//    any counter twice.
	if _, dup := e.seen[ev.ID]; dup {
		s := e.stats[key]
		s.DuplicateEvents++
		e.stats[key] = s
		return model.IngestResult{Duplicate: true, Reason: "duplicate event id: " + ev.ID}
	}

	// 2) Lateness window against the announced watermark. An event is expired
	//    only when event_time is strictly older than watermark-tolerance;
	//    the boundary itself is accepted. With no watermark yet, nothing
	//    expires. Expired events are recorded (countable, explainable) and
	//    their IDs are still remembered so redelivery is seen as duplicate.
	if wm, ok := e.watermark[key]; ok {
		boundary := wm.Add(-e.cfg.LatenessTolerance)
		if ev.EventTime.Before(boundary) {
			e.seen[ev.ID] = ev.EventTime.UnixNano()
			s := e.ensureStats(key, ev)
			s.ExpiredEvents++
			e.stats[key] = s
			return model.IngestResult{
				Expired:   true,
				Reason:    fmt.Sprintf("expired: event_time %s before watermark boundary %s", ev.EventTime.Format(time.RFC3339Nano), boundary.Format(time.RFC3339Nano)),
				Watermark: wm,
			}
		}
	}

	// 3) Accepted: merge. Event-time comparisons decide ordering; processing
	//    time only updates monotonic freshness/ingest bookkeeping.
	e.seen[ev.ID] = ev.EventTime.UnixNano()
	s := e.ensureStats(key, ev)
	if ev.EventTime.After(s.LatestEventTime) {
		s.LatestEventTime = ev.EventTime
	}
	if now.After(s.LastIngestAt) {
		s.LastIngestAt = now
	}
	switch ev.Type {
	case model.EventTaskStarted:
		s.Started++
		mergeRun(&s, ev)
	case model.EventTaskSucceeded:
		s.Succeeded++
		mergeRun(&s, ev)
	case model.EventTaskFailed:
		s.Failed++
		mergeRun(&s, ev)
	case model.EventRecordWritten:
		s.Records += ev.Records
		if now.After(s.LastFreshAt) {
			s.LastFreshAt = now
		}
	case model.EventWatermark:
		if ev.EventTime.After(s.Watermark) {
			s.Watermark = ev.EventTime
			e.watermark[key] = ev.EventTime
		}
		if now.After(s.LastFreshAt) {
			s.LastFreshAt = now
		}
	}
	e.stats[key] = s
	return model.IngestResult{Accepted: true, Watermark: s.Watermark}
}

// ensureStats returns a stats value for key, seeding identity fields once.
func (e *Engine) ensureStats(key string, ev model.Event) model.OperatorStats {
	s, ok := e.stats[key]
	if !ok {
		s = model.OperatorStats{
			PipelineID: ev.PipelineID,
			OperatorID: ev.OperatorID,
			Runs:       map[string]model.RunStateInfo{},
		}
	}
	if s.Runs == nil {
		s.Runs = map[string]model.RunStateInfo{}
	}
	return s
}

// mergeRun records a run lifecycle transition using the same ordering rule as
// the evaluator: greater event time wins; at equal event time the terminal
// rank decides. The result therefore cannot depend on arrival order.
func mergeRun(s *model.OperatorStats, ev model.Event) {
	state := model.RunStarted
	switch ev.Type {
	case model.EventTaskSucceeded:
		state = model.RunSucceeded
	case model.EventTaskFailed:
		state = model.RunFailed
	}
	cand := model.RunStateInfo{State: state, At: ev.EventTime, Error: ev.Error}
	if cur, ok := s.Runs[ev.RunID]; ok {
		c := cand
		if c.At.Compare(cur.At) < 0 ||
			(c.At.Compare(cur.At) == 0 && terminalRank(c.State) <= terminalRank(cur.State)) {
			return
		}
	}
	s.Runs[ev.RunID] = cand
}

func isLifecycle(t model.EventType) bool {
	return t == model.EventTaskStarted || t == model.EventTaskSucceeded || t == model.EventTaskFailed
}

// Stats returns a deep copy of the current operator aggregates.
func (e *Engine) Stats() map[string]model.OperatorStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]model.OperatorStats, len(e.stats))
	for k, v := range e.stats {
		if v.Runs != nil {
			runs := make(map[string]model.RunStateInfo, len(v.Runs))
			for rk, rv := range v.Runs {
				runs[rk] = rv
			}
			v.Runs = runs
		}
		out[k] = v
	}
	return out
}

// Alerts returns the live alert map (values must be treated as read-only).
func (e *Engine) Alerts() map[string]*model.Alert {
	return e.alerts.Snapshot()
}

// Evaluate recomputes all conditions and applies them to the alert manager.
// It returns the alerts that transitioned during this pass (new/re-fired or
// resolved); suppressed and unchanged conditions produce no entry.
func (e *Engine) Evaluate() []*model.Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.nowLocked()
	conds := evaluateConditions(e.cfg, e.stats, now)
	var changed []*model.Alert
	for _, c := range conds {
		if a := e.alerts.Apply(c); a != nil {
			changed = append(changed, a)
		}
	}
	return changed
}

// AddSilence mutes a fingerprint ("*" = global) over [start, end).
func (e *Engine) AddSilence(fp, comment string, start, end time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.alerts.AddSilence(fp, comment, start, end)
}

// nowLocked returns a processing-time instant that is monotonic across clock
// rollback: it tracks the greatest wall-clock reading seen so far and stays
// flat (never goes backwards) when the wall clock steps back.
func (e *Engine) nowLocked() time.Time {
	t := e.clk.Now()
	if t.After(e.monoNow) {
		e.monoNow = t
	}
	return e.monoNow
}

// Save writes the full state snapshot atomically.
func (e *Engine) Save() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.st == nil {
		return nil
	}
	now := e.nowLocked()
	snap := store.NewSnapshot(now)
	snap.Stats = e.stats
	snap.SeenEvents = e.seen
	snap.Watermarks = e.watermark
	snap.Alerts = e.alerts.Snapshot()
	for k, s := range e.alerts.Silences() {
		snap.Silences[k] = store.Silence{
			Fingerprint: s.Fingerprint,
			Comment:     s.Comment,
			StartsAt:    s.StartsAt,
			EndsAt:      s.EndsAt,
		}
	}
	if err := e.st.Save(snap); err != nil {
		return err
	}
	e.savedAt = now
	return nil
}

// Close persists pending state and releases resources.
func (e *Engine) Close() error {
	return e.Save()
}
