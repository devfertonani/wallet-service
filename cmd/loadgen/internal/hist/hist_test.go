package hist

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func within(t *testing.T, name string, got, want time.Duration, rel float64) {
	t.Helper()
	if math.Abs(float64(got-want)) > rel*float64(want) {
		t.Errorf("%s = %v, want %v (±%.1f%%)", name, got, want, rel*100)
	}
}

func TestQuantilesUniform(t *testing.T) {
	h := New()
	for i := 1; i <= 10000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	within(t, "p50", h.Quantile(0.50), 5000*time.Millisecond, 0.02)
	within(t, "p95", h.Quantile(0.95), 9500*time.Millisecond, 0.02)
	within(t, "p99", h.Quantile(0.99), 9900*time.Millisecond, 0.02)
	if h.Max() != 10*time.Second {
		t.Errorf("max = %v", h.Max())
	}
	if h.Min() != time.Millisecond {
		t.Errorf("min = %v", h.Min())
	}
	within(t, "mean", h.Mean(), 5000500*time.Microsecond, 0.001)
}

func TestExactBelowSubCount(t *testing.T) {
	h := New()
	for i := 0; i < 64; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}
	if got := h.Quantile(0.5); got != 31*time.Microsecond {
		t.Errorf("p50 = %v, want 31µs", got)
	}
	if got := h.Quantile(1); got != 63*time.Microsecond {
		t.Errorf("p100 = %v", got)
	}
}

func TestBucketBoundsAreConsistent(t *testing.T) {
	for _, v := range []int64{0, 1, 63, 64, 65, 127, 128, 1000, 123456, 1 << 40, math.MaxInt64 / 2} {
		i := index(v)
		if upper(i) < v {
			t.Fatalf("value %d above its bucket upper %d", v, upper(i))
		}
		if i > 0 && upper(i-1) >= v {
			t.Fatalf("value %d also fits the previous bucket (upper %d)", v, upper(i-1))
		}
		if v >= subCount && float64(upper(i)-v) > float64(v)/subCount {
			t.Fatalf("relative error too high for %d: upper %d", v, upper(i))
		}
	}
}

func TestSingleValueAndEmpty(t *testing.T) {
	h := New()
	if h.Quantile(0.99) != 0 || h.Max() != 0 || h.Mean() != 0 {
		t.Fatal("empty histogram must report zeros")
	}
	h.Record(250 * time.Millisecond)
	for _, q := range []float64{0, 0.5, 0.99, 1} {
		if got := h.Quantile(q); got != 250*time.Millisecond {
			t.Errorf("q%.2f = %v, want exactly 250ms (capped at min/max)", q, got)
		}
	}
	h.Record(-time.Second)
	if h.Min() != 0 {
		t.Errorf("negative must record as zero, min = %v", h.Min())
	}
}

func TestMergeAndJSONRoundTrip(t *testing.T) {
	a, b := New(), New()
	for i := 1; i <= 100; i++ {
		a.Record(time.Duration(i) * time.Millisecond)
		b.Record(time.Duration(i+100) * time.Millisecond)
	}
	a.Merge(b)
	if a.Count() != 200 || a.Max() != 200*time.Millisecond || a.Min() != time.Millisecond {
		t.Fatalf("merge: count %d max %v min %v", a.Count(), a.Max(), a.Min())
	}
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var c Histogram
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if c.Summary() != a.Summary() {
		t.Fatalf("round trip changed summary: %+v vs %+v", c.Summary(), a.Summary())
	}
	if err := json.Unmarshal([]byte(`{"buckets":[[1,2]],"count":3}`), &c); err == nil {
		t.Fatal("inconsistent count must be rejected")
	}
}

func TestRecorderSnapshotIsSortedCopy(t *testing.T) {
	r := NewRecorder()
	r.Record(Key{"sqs", "BET", "sent"}, time.Millisecond)
	r.Record(Key{"http", "WIN", "PROCESSED"}, time.Millisecond)
	r.Record(Key{"http", "BET", "PROCESSED"}, 2*time.Millisecond)
	s := r.Snapshot()
	if len(s) != 3 || s[0].Kind != "BET" || s[0].Channel != "http" || s[2].Channel != "sqs" {
		t.Fatalf("unexpected order: %+v", s)
	}
	s[0].Histogram.Record(time.Hour)
	if r.Snapshot()[0].Histogram.Max() == time.Hour {
		t.Fatal("snapshot must be a copy")
	}
}
