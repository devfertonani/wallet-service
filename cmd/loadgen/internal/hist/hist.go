// Package hist is a small log-linear latency histogram, in the spirit of
// HdrHistogram: values are recorded in microseconds, exact below 64 µs and
// with at most 1/64 (~1.6%) relative error above, in constant memory.
// It exists so the load generator does not need an extra module.
package hist

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"time"
)

const (
	subBits  = 6
	subCount = 1 << subBits // buckets per power of two
	// 64 exact buckets plus subCount buckets for each shift 0..57.
	bucketCount = subCount + (64-subBits)*subCount
)

// Histogram is not safe for concurrent use; Recorder wraps it with a lock.
type Histogram struct {
	counts []uint64
	count  uint64
	sum    float64 // microseconds; float only for the mean, never for money
	min    int64
	max    int64
}

func New() *Histogram {
	return &Histogram{counts: make([]uint64, bucketCount), min: math.MaxInt64}
}

func index(v int64) int {
	if v < subCount {
		return int(v)
	}
	msb := bits.Len64(uint64(v)) - 1
	shift := msb - subBits
	return subCount + shift*subCount + int(v>>uint(shift)) - subCount
}

// upper returns the highest value that maps to bucket i.
func upper(i int) int64 {
	if i < subCount {
		return int64(i)
	}
	shift := (i - subCount) / subCount
	sub := (i-subCount)%subCount + subCount
	return (int64(sub+1) << uint(shift)) - 1
}

// Record adds one observation. Negative durations are recorded as zero.
func (h *Histogram) Record(d time.Duration) {
	v := d.Microseconds()
	if v < 0 {
		v = 0
	}
	h.counts[index(v)]++
	h.count++
	h.sum += float64(v)
	if v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
}

func (h *Histogram) Count() uint64 { return h.count }

func (h *Histogram) Max() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.max) * time.Microsecond
}

func (h *Histogram) Min() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.min) * time.Microsecond
}

func (h *Histogram) Mean() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.sum/float64(h.count)) * time.Microsecond
}

// Quantile returns the value at quantile q (0..1): the highest value of the
// bucket holding the ceil(q*count)-th observation, capped at the true max.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	if q <= 0 {
		return h.Min()
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	if rank > h.count {
		rank = h.count
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			v := upper(i)
			if v > h.max {
				v = h.max
			}
			if v < h.min {
				v = h.min
			}
			return time.Duration(v) * time.Microsecond
		}
	}
	return h.Max()
}

// Merge adds every observation of o into h.
func (h *Histogram) Merge(o *Histogram) {
	if o == nil || o.count == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.count += o.count
	h.sum += o.sum
	if o.min < h.min {
		h.min = o.min
	}
	if o.max > h.max {
		h.max = o.max
	}
}

// Summary is the human facing digest of a histogram, in milliseconds.
type Summary struct {
	Count  uint64  `json:"count"`
	MeanMs float64 `json:"meanMs"`
	P50Ms  float64 `json:"p50Ms"`
	P95Ms  float64 `json:"p95Ms"`
	P99Ms  float64 `json:"p99Ms"`
	P999Ms float64 `json:"p999Ms"`
	MaxMs  float64 `json:"maxMs"`
}

func ms(d time.Duration) float64 { return math.Round(float64(d.Microseconds())) / 1000 }

func (h *Histogram) Summary() Summary {
	return Summary{
		Count:  h.count,
		MeanMs: ms(h.Mean()),
		P50Ms:  ms(h.Quantile(0.50)),
		P95Ms:  ms(h.Quantile(0.95)),
		P99Ms:  ms(h.Quantile(0.99)),
		P999Ms: ms(h.Quantile(0.999)),
		MaxMs:  ms(h.Max()),
	}
}

// wire is the sparse JSON form: [[bucket, count], ...] plus the exact stats.
type wire struct {
	Buckets [][2]uint64 `json:"buckets"`
	Count   uint64      `json:"count"`
	SumUs   float64     `json:"sumUs"`
	MinUs   int64       `json:"minUs"`
	MaxUs   int64       `json:"maxUs"`
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
	w := wire{Count: h.count, SumUs: h.sum, MinUs: h.min, MaxUs: h.max, Buckets: [][2]uint64{}}
	if h.count == 0 {
		w.MinUs = 0
	}
	for i, c := range h.counts {
		if c > 0 {
			w.Buckets = append(w.Buckets, [2]uint64{uint64(i), c})
		}
	}
	return json.Marshal(w)
}

func (h *Histogram) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	*h = *New()
	var total uint64
	for _, p := range w.Buckets {
		if p[0] >= bucketCount {
			return fmt.Errorf("hist: bucket %d out of range", p[0])
		}
		h.counts[p[0]] += p[1]
		total += p[1]
	}
	if total != w.Count {
		return fmt.Errorf("hist: bucket total %d does not match count %d", total, w.Count)
	}
	h.count, h.sum, h.max = w.Count, w.SumUs, w.MaxUs
	if w.Count > 0 {
		h.min = w.MinUs
	}
	return nil
}
