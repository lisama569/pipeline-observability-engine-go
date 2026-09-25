package model

import "time"

// AlertReason distinguishes the root causes an alert can be raised for.
type AlertReason string

const (
	ReasonDataLag     AlertReason = "data_lag"
	ReasonTaskTimeout AlertReason = "task_timeout"
	ReasonTaskFailure AlertReason = "task_failure"
)

// AlertState is active or resolved.
type AlertState string

const (
	AlertActive   AlertState = "active"
	AlertResolved AlertState = "resolved"
)

// Alert is a deduplicated alert keyed by its fingerprint.
type Alert struct {
	Fingerprint     string      `json:"fingerprint"`
	PipelineID      string      `json:"pipeline_id"`
	OperatorID      string      `json:"operator_id"`
	Reason          AlertReason `json:"reason"`
	State           AlertState  `json:"state"`
	Message         string      `json:"message"`
	FirstFiredAt    time.Time   `json:"first_fired_at"`
	LastFiredAt     time.Time   `json:"last_fired_at"`
	ResolvedAt      *time.Time  `json:"resolved_at,omitempty"`
	SuppressedUntil time.Time   `json:"suppressed_until,omitempty"`
	FiredCount      int         `json:"fired_count"`
}

// Condition is the evaluated health state of one (operator, reason) pair.
type Condition struct {
	PipelineID string      `json:"pipeline_id"`
	OperatorID string      `json:"operator_id"`
	Reason     AlertReason `json:"reason"`
	Active     bool        `json:"active"`
	Message    string      `json:"message"`
	At         time.Time   `json:"at"`
}

// Run lifecycle states.
const (
	RunStarted   = "started"
	RunSucceeded = "succeeded"
	RunFailed    = "failed"
)

// RunStateInfo is the event-time-ordered lifecycle state of one run.
type RunStateInfo struct {
	State string    `json:"state"`
	At    time.Time `json:"at"`
	Error string    `json:"error,omitempty"`
}

// OperatorStats is the aggregated view for one operator.
type OperatorStats struct {
	PipelineID string `json:"pipeline_id"`
	OperatorID string `json:"operator_id"`

	Started   int64 `json:"started"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Records   int64 `json:"records"`

	// LatestEventTime is the max event time of accepted events.
	LatestEventTime time.Time `json:"latest_event_time"`
	// Watermark is the max watermark announced via watermark events.
	Watermark time.Time `json:"watermark"`

	// LastIngestAt is the clock (processing) time of the latest accepted
	// event of any kind; LastFreshAt is the clock time of the latest accepted
	// freshness-bearing event (record_written / watermark). Both are used for
	// data-freshness evaluation and are monotonic across clock rollback.
	LastIngestAt time.Time `json:"last_ingest_at"`
	LastFreshAt  time.Time `json:"last_fresh_at"`

	ExpiredEvents   int64 `json:"expired_events"`
	DuplicateEvents int64 `json:"duplicate_events"`

	// Runs maps runID -> lifecycle state, ordered by event time so that
	// out-of-order arrival cannot change the final conclusion.
	Runs map[string]RunStateInfo `json:"runs"`
}
