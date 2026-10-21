package sched

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestConstantPlanIsEvenlySpaced(t *testing.T) {
	offs := Plan{Duration: 10 * time.Second, Rate: Constant(100)}.Offsets()
	if len(offs) != 1000 {
		t.Fatalf("arrivals = %d, want 1000", len(offs))
	}
	for i := 1; i < len(offs); i++ {
		if d := offs[i] - offs[i-1]; d != 10*time.Millisecond {
			t.Fatalf("gap %d = %v, want 10ms", i, d)
		}
	}
}

func TestStepsPlanCountsPerStep(t *testing.T) {
	every := 2 * time.Second
	p := Plan{Duration: 3 * every, Rate: Steps(50, 50, 120, every)}
	perStep := map[int]int{}
	for _, o := range p.Offsets() {
		perStep[StepIndex(o, every)]++
	}
	// 50/s, 100/s, then capped at 120/s, each for 2 s (±1 at the boundary).
	for step, want := range map[int]int{0: 100, 1: 200, 2: 240} {
		if got := perStep[step]; got < want-1 || got > want+1 {
			t.Errorf("step %d: %d arrivals, want ~%d", step, got, want)
		}
	}
}

func TestZeroRatePausesInsteadOfSpinning(t *testing.T) {
	offs := Plan{Duration: time.Second, Rate: Constant(0)}.Offsets()
	if len(offs) != 10 {
		t.Fatalf("zero rate must advance in 100ms pauses, got %d arrivals", len(offs))
	}
}

func TestRunKeepsPaceAndReportsScheduledInstants(t *testing.T) {
	start := time.Now()
	var fired atomic.Int64
	var lastAt time.Time
	st := Run(context.Background(), start, Plan{Duration: 300 * time.Millisecond, Rate: Constant(200)},
		func(seq int64, at time.Time, lag time.Duration) {
			if want := start.Add(time.Duration(seq) * 5 * time.Millisecond); !at.Equal(want) {
				t.Errorf("arrival %d scheduled at %v, want %v", seq, at.Sub(start), want.Sub(start))
			}
			lastAt = at
			fired.Add(1)
		})
	if st.Arrivals != 60 || fired.Load() != 60 {
		t.Fatalf("arrivals = %d, fired = %d, want 60", st.Arrivals, fired.Load())
	}
	if lastAt.Sub(start) != 295*time.Millisecond {
		t.Fatalf("last arrival at %v", lastAt.Sub(start))
	}
	if elapsed := time.Since(start); elapsed < 290*time.Millisecond {
		t.Fatalf("run finished in %v: it did not wait for the schedule", elapsed)
	}
	if st.MaxLag > 100*time.Millisecond {
		t.Fatalf("lag %v on an idle machine", st.MaxLag)
	}
}

func TestRunCatchesUpWithoutDroppingArrivals(t *testing.T) {
	// A start in the past means every arrival is already late: the scheduler
	// must still fire all of them (with their original instants) and report
	// the lag, never compress the schedule.
	start := time.Now().Add(-time.Second)
	st := Run(context.Background(), start, Plan{Duration: 500 * time.Millisecond, Rate: Constant(100)},
		func(int64, time.Time, time.Duration) {})
	if st.Arrivals != 50 {
		t.Fatalf("arrivals = %d, want 50", st.Arrivals)
	}
	if st.MaxLag < time.Second {
		t.Fatalf("lag %v must reflect the late start", st.MaxLag)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	st := Run(ctx, time.Now(), Plan{Duration: time.Hour, Rate: Constant(10)}, func(int64, time.Time, time.Duration) {})
	if st.Arrivals > 2 {
		t.Fatalf("arrivals after cancel: %d", st.Arrivals)
	}
}
