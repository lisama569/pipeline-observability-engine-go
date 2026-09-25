package model

import (
	"errors"
	"time"
)

// EventType enumerates pipeline runtime events.
type EventType string

const (
	EventTaskStarted   EventType = "task_started"
	EventTaskSucceeded EventType = "task_succeeded"
	EventTaskFailed    EventType = "task_failed"
	EventRecordWritten EventType = "record_written"
	EventWatermark     EventType = "watermark"
)

// Event is a single runtime telemetry event of a pipeline operator.
//
// Two timelines exist:
//   - EventTime: when the thing logically happened (may arrive late/out of
//     order; the ordering authority for aggregation and windows).
//   - ingest time: when the engine received it (from the injected Clock),
//     used only for data-freshness bookkeeping.
type Event struct {
	ID         string    `json:"id"`
	PipelineID string    `json:"pipeline_id"`
	OperatorID string    `json:"operator_id"`
	RunID      string    `json:"run_id"`
	Type       EventType `json:"type"`
	EventTime  time.Time `json:"event_time"`
	Records    int64     `json:"records,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// StatsKey is the aggregation key for one operator.
func StatsKey(pipelineID, operatorID string) string {
	return pipelineID + "/" + operatorID
}

// Key returns the operator aggregation key of the event.
func (e Event) Key() string { return StatsKey(e.PipelineID, e.OperatorID) }

// Validate checks the minimal invariants required for deterministic handling.
func (e Event) Validate() error {
	switch {
	case e.ID == "":
		return errors.New("event id is required for dedup")
	case e.PipelineID == "" || e.OperatorID == "":
		return errors.New("pipeline id and operator id are required")
	case e.EventTime.IsZero():
		return errors.New("event time is required")
	}
	switch e.Type {
	case EventTaskStarted, EventTaskSucceeded, EventTaskFailed,
		EventRecordWritten, EventWatermark:
	default:
		return errors.New("unknown event type: " + string(e.Type))
	}
	if e.Type == EventTaskFailed && e.Error == "" {
		return errors.New("task_failed event requires an error message")
	}
	return nil
}

// IngestResult reports how an ingested event was classified. Exactly one of
// Accepted/Duplicate/Expired is true for a well-formed event; malformed events
// have Accepted=false with a Reason and none of the flags set.
type IngestResult struct {
	Accepted  bool      `json:"accepted"`
	Duplicate bool      `json:"duplicate"`
	Expired   bool      `json:"expired"`
	Reason    string    `json:"reason,omitempty"`
	Watermark time.Time `json:"watermark,omitempty"`
}
