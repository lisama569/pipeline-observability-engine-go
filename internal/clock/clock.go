package clock

import "time"

// Clock abstracts time sources so tests can use a deterministic fake clock,
// including clock-rollback (NTP step back) scenarios.
type Clock interface {
	Now() time.Time
}

// RealClock uses the local wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a manually controlled clock used in tests. Time may be moved
// both forwards and backwards (Set allows simulating clock rollback).
type FakeClock struct {
	now time.Time
}

func NewFake(start time.Time) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) Now() time.Time { return c.now }

// Advance moves the clock forward (negative durations are ignored; use Set
// to deliberately simulate a backwards step).
func (c *FakeClock) Advance(d time.Duration) {
	if d > 0 {
		c.now = c.now.Add(d)
	}
}

// Set assigns an arbitrary instant, including one in the past.
func (c *FakeClock) Set(t time.Time) { c.now = t }
