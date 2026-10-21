// Package sched is an open-model (constant arrival rate) scheduler.
//
// Each arrival has a scheduled instant computed from the rate alone, never
// from how fast the system under test answers. Callers measure latency from
// that instant, so a slow server shows up in the percentiles instead of
// silently slowing the generator down (coordinated omission).
package sched

import (
	"context"
	"time"
)

// RateFunc returns the target arrival rate, in arrivals per second, at a
// given offset from the start of the run.
type RateFunc func(elapsed time.Duration) float64

// Constant is a fixed rate.
func Constant(perSecond float64) RateFunc {
	return func(time.Duration) float64 { return perSecond }
}

// Steps starts at start and adds step every interval, never above max.
func Steps(start, step, max float64, every time.Duration) RateFunc {
	return func(e time.Duration) float64 {
		r := start + step*float64(e/every)
		if r > max {
			return max
		}
		return r
	}
}

// StepIndex is the ramp step an offset belongs to.
func StepIndex(e, every time.Duration) int {
	if every <= 0 {
		return 0
	}
	return int(e / every)
}

// Plan is a rate over a fixed duration.
type Plan struct {
	Duration time.Duration
	Rate     RateFunc
}

// Next returns the offset of the arrival after prev, or false once the plan
// is over. Rates at or below zero pause the plan for 100 ms.
func (p Plan) Next(prev time.Duration, first bool) (time.Duration, bool) {
	var next time.Duration
	if first {
		next = 0
	} else {
		r := p.Rate(prev)
		if r <= 0 {
			next = prev + 100*time.Millisecond
		} else {
			next = prev + time.Duration(float64(time.Second)/r)
		}
	}
	if next >= p.Duration {
		return 0, false
	}
	return next, true
}

// Offsets lists every arrival offset of the plan; for tests and dry runs.
func (p Plan) Offsets() []time.Duration {
	var out []time.Duration
	var cur time.Duration
	first := true
	for {
		n, ok := p.Next(cur, first)
		if !ok {
			return out
		}
		out = append(out, n)
		cur, first = n, false
	}
}

// Stats describe how well the generator kept the schedule.
type Stats struct {
	Arrivals int64
	// MaxLag is the worst delay between a scheduled instant and the moment
	// the arrival was handed to fire. A large lag means the generator, not
	// the system under test, was the bottleneck.
	MaxLag time.Duration
}

// Run fires one arrival per scheduled instant until the plan ends or ctx is
// cancelled. fire receives the arrival sequence number, its scheduled
// instant and the lag; it must not block (start a goroutine or drop).
func Run(ctx context.Context, start time.Time, p Plan, fire func(seq int64, at time.Time, lag time.Duration)) Stats {
	var st Stats
	var cur time.Duration
	first := true
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		off, ok := p.Next(cur, first)
		if !ok {
			return st
		}
		cur, first = off, false
		at := start.Add(off)
		if wait := time.Until(at); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				return st
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return st
		}
		lag := time.Since(at)
		if lag < 0 {
			lag = 0
		}
		if lag > st.MaxLag {
			st.MaxLag = lag
		}
		fire(st.Arrivals, at, lag)
		st.Arrivals++
	}
}
