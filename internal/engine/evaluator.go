package engine

import (
	"fmt"
	"time"

	"github.com/lisama569/pipeline-observability-engine-go/internal/model"
)

// evaluateConditions derives the current conditions from operator aggregates.
// For every known operator it always emits exactly one condition per reason
// (data_lag, task_timeout, task_failure), active or not; this closed set is
// what lets the alert manager observe recovery and resolve alerts.
//
// The function is pure in (config, stats, now): identical input always yields
// identical, explainable output, regardless of event arrival interleaving.
//
// Clock-rollback policy: every elapsed duration is clamped at >= 0, so a
// backwards clock step can neither fabricate a timeout/lag nor erase a
// recorded failure.
//
// Boundary policy: a threshold is crossed only when elapsed is STRICTLY
// greater than the threshold; elapsed == threshold is still healthy.
func evaluateConditions(cfg model.Config, stats map[string]model.OperatorStats, now time.Time) []model.Condition {
	var out []model.Condition
	for _, k := range sortedStatsKeys(stats) {
		s := stats[k]
		c := model.Condition{
			PipelineID: s.PipelineID,
			OperatorID: s.OperatorID,
			At:         now,
		}

		// Latest run by event time; terminal state wins deterministically on
		// an exact event-time tie (failed > succeeded > started).
		runID, run := latestRun(s)

		failure := c
		failure.Reason = model.ReasonTaskFailure
		if run != nil && run.State == model.RunFailed {
			failure.Active = true
			failure.Message = fmt.Sprintf("run %s failed at %s: %s",
				runID, run.At.Format(time.RFC3339Nano), run.Error)
		} else if run != nil {
			failure.Message = fmt.Sprintf("healthy: latest run %s is %s (since %s)",
				runID, run.State, run.At.Format(time.RFC3339Nano))
		} else {
			failure.Message = "healthy: no failed run"
		}
		out = append(out, failure)

		timeout := c
		timeout.Reason = model.ReasonTaskTimeout
		if run != nil && run.State == model.RunStarted {
			elapsed := clampDuration(now.Sub(run.At))
			if elapsed > cfg.TaskTimeout {
				timeout.Active = true
				timeout.Message = fmt.Sprintf(
					"run %s has been running %s (> timeout %s) since %s",
					runID, elapsed, cfg.TaskTimeout, run.At.Format(time.RFC3339Nano))
			} else {
				timeout.Message = fmt.Sprintf(
					"healthy: run %s running %s (<= timeout %s)",
					runID, elapsed, cfg.TaskTimeout)
			}
		} else {
			timeout.Message = "healthy: no running task"
		}
		out = append(out, timeout)

		lag := c
		lag.Reason = model.ReasonDataLag
		if s.LastFreshAt.IsZero() {
			lag.Message = "healthy: no data observed yet"
		} else {
			sinceFresh := clampDuration(now.Sub(s.LastFreshAt))
			if sinceFresh > cfg.DataLagThreshold {
				lag.Active = true
				lag.Message = fmt.Sprintf(
					"no fresh data for %s (> threshold %s); last fresh at %s",
					sinceFresh, cfg.DataLagThreshold, s.LastFreshAt.Format(time.RFC3339Nano))
			} else {
				lag.Message = fmt.Sprintf(
					"healthy: fresh data %s ago (<= threshold %s)",
					sinceFresh, cfg.DataLagThreshold)
			}
		}
		out = append(out, lag)
	}
	return out
}

// latestRun returns the run with the greatest event time; on equal event
// times a terminal state (failed > succeeded) deterministically beats started,
// so the conclusion does not depend on Go map iteration order.
func latestRun(s model.OperatorStats) (string, *model.RunStateInfo) {
	bestID := ""
	var best *model.RunStateInfo
	for id, r := range s.Runs {
		r := r
		rcand := &r
		if best == nil || runLess(best, rcand) {
			bestID = id
			best = rcand
		}
	}
	return bestID, best
}

// runLess reports whether b is a strictly "later" run state than a.
func runLess(a, b *model.RunStateInfo) bool {
	if c := b.At.Compare(a.At); c != 0 {
		return c > 0
	}
	return terminalRank(b.State) > terminalRank(a.State)
}

func terminalRank(state string) int {
	switch state {
	case model.RunFailed:
		return 3
	case model.RunSucceeded:
		return 2
	default:
		return 1
	}
}

func clampDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}
