package model

import (
	"errors"
	"time"
)

// Config holds all configurable windows and thresholds.
//
// Boundary policy (applied identically everywhere, see docs):
//   - comparisons against a threshold use STRICT GREATER-THAN: a value exactly
//     equal to the threshold is still "within" the window / "not yet timed out".
//   - lateness uses a closed boundary: event_time >= watermark-LatenessTolerance
//     is accepted (an event exactly at the boundary is NOT expired).
type Config struct {
	// LatenessTolerance: when a watermark is known for an operator, events
	// whose event_time is older than (watermark - LatenessTolerance) are
	// marked expired. When no watermark exists yet, nothing can expire.
	LatenessTolerance time.Duration
	// DataLagThreshold: if the operator has no accepted event carrying data
	// freshness (record_written / watermark) for longer than this relative
	// to the evaluation clock, condition data_lag is active.
	DataLagThreshold time.Duration
	// TaskTimeout: a run in state "started" whose duration (clock now minus
	// its started event's event time, clamped against rollback) is strictly
	// greater than this is condition task_timeout.
	TaskTimeout time.Duration
	// AlertSuppressWindow: while an alert is active, repeated bad conditions
	// for the same fingerprint only refresh the alert (no new firing) until
	// this window since the last firing elapses; after the alert has been
	// resolved it can fire again immediately on re-deterioration.
	AlertSuppressWindow time.Duration
	// EvaluationInterval is informational; callers drive Evaluate themselves.
	EvaluationInterval time.Duration
}

// DefaultConfig returns local-reproducible defaults.
func DefaultConfig() Config {
	return Config{
		LatenessTolerance:   30 * time.Second,
		DataLagThreshold:    60 * time.Second,
		TaskTimeout:         5 * time.Minute,
		AlertSuppressWindow: 10 * time.Minute,
		EvaluationInterval:  time.Second,
	}
}

// Validate rejects nonsensical configurations so semantics stay unambiguous.
func (c Config) Validate() error {
	if c.LatenessTolerance < 0 {
		return errors.New("LatenessTolerance must be >= 0")
	}
	if c.DataLagThreshold <= 0 {
		return errors.New("DataLagThreshold must be > 0")
	}
	if c.TaskTimeout <= 0 {
		return errors.New("TaskTimeout must be > 0")
	}
	if c.AlertSuppressWindow < 0 {
		return errors.New("AlertSuppressWindow must be >= 0")
	}
	return nil
}
