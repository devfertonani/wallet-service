package hist

import (
	"sort"
	"sync"
	"time"
)

// Key identifies one latency series: channel (http, sqs), operation kind and
// final outcome (PROCESSED, REJECTED:INSUFFICIENT_FUNDS, http_503, ...).
type Key struct {
	Channel string `json:"channel"`
	Kind    string `json:"kind"`
	Outcome string `json:"outcome"`
}

// Series is one histogram with its key, as stored in run.json.
type Series struct {
	Key
	Histogram *Histogram `json:"histogram"`
}

// Recorder is a concurrency-safe set of histograms keyed by Key.
type Recorder struct {
	mu sync.Mutex
	m  map[Key]*Histogram
}

func NewRecorder() *Recorder { return &Recorder{m: map[Key]*Histogram{}} }

func (r *Recorder) Record(k Key, d time.Duration) {
	r.mu.Lock()
	h, ok := r.m[k]
	if !ok {
		h = New()
		r.m[k] = h
	}
	h.Record(d)
	r.mu.Unlock()
}

// Snapshot returns a deep copy of every series, sorted by key.
func (r *Recorder) Snapshot() []Series {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Series, 0, len(r.m))
	for k, h := range r.m {
		c := New()
		c.Merge(h)
		out = append(out, Series{Key: k, Histogram: c})
	}
	SortSeries(out)
	return out
}

func SortSeries(s []Series) {
	sort.Slice(s, func(i, j int) bool {
		a, b := s[i].Key, s[j].Key
		if a.Channel != b.Channel {
			return a.Channel < b.Channel
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Outcome < b.Outcome
	})
}
