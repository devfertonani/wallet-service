// Package scenario turns a scenario profile into load: it seeds wallets,
// schedules tasks at a constant arrival rate and records every operation in
// the journal and in the latency histograms.
package scenario

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Task types. A task is one unit of arrival; it may send several operations.
const (
	TaskRoundHTTP     = "round-http"     // BET then WIN/LOSS/REFUND/ROLLBACK, over HTTP
	TaskRoundSQS      = "round-sqs"      // the same round, over SQS
	TaskHot           = "hot"            // one BET or WIN on one of the few hot wallets
	TaskDuplicate     = "duplicate"      // one BET fanned out over HTTP+SQS, plus 409 probes
	TaskReversalFirst = "reversal-first" // REFUND/ROLLBACK before its BET
)

// opsPerTask is the expected number of sends of each task, used to turn the
// target operation rate into a task arrival rate.
var opsPerTask = map[string]float64{
	TaskRoundHTTP:     2,
	TaskRoundSQS:      2,
	TaskHot:           1,
	TaskDuplicate:     DupHTTPCopies + DupSQSSameID + DupSQSDistinct + 2,
	TaskReversalFirst: 2,
}

// Copies of each duplicated operation: HTTP copies are spread over the
// instances; SQS copies either repeat the same messageId (broker and inbox
// dedup) or use distinct messageIds (idempotency key dedup).
const (
	DupHTTPCopies  = 6
	DupSQSSameID   = 2
	DupSQSDistinct = 2
)

// Profile is a scenario with its defaults; every field can be overridden by
// a flag.
type Profile struct {
	Name        string
	Description string

	Wallets       int   // regular wallets to seed
	OpeningMinor  int64 // opening balance of regular wallets
	HotWallets    int   // hot wallets to seed (hot-wallet scenario)
	HotOpenMinor  int64
	Rate          float64 // target operations per second
	Duration      time.Duration
	Warmup        time.Duration
	Mix           map[string]float64
	RampStep      float64       // ops/s added per step (ramp only)
	RampEvery     time.Duration // step length (ramp only)
	RampMax       float64
	MaxDelay      time.Duration // reversal-first: delay before the BET
	OrphanRatio   float64       // reversal-first: reversals whose BET never comes
	DupSpread     time.Duration // duplicate: copies are spread over this window
	MaxInFlight   int
	DrainTimeout  time.Duration
	ResolveWithin time.Duration // reversal-first: max wait for resolution
}

var profiles = map[string]Profile{
	"smoke": {
		Description: "Tiny run of every task type, to check that the environment works end to end.",
		Wallets:     50, OpeningMinor: 100000, Rate: 20, Duration: 30 * time.Second, Warmup: 5 * time.Second,
		Mix: map[string]float64{TaskRoundHTTP: 70, TaskRoundSQS: 20, TaskDuplicate: 5, TaskReversalFirst: 5},
	},
	"steady": {
		// 30 op/s stays well below the MiniStack ceiling measured on the
		// reference machine (saturation from ~60 op/s); see loadtest/README.md.
		Description: "Realistic constant load: rounds BET then WIN/LOSS, 10% REFUND/ROLLBACK, 80% HTTP / 20% SQS.",
		Wallets:     1000, OpeningMinor: 100000, Rate: 30, Duration: 2 * time.Minute, Warmup: 20 * time.Second,
		Mix: map[string]float64{TaskRoundHTTP: 80, TaskRoundSQS: 20},
	},
	"hot-wallet": {
		Description: "Many concurrent operations on a few wallets over the 3 instances: row lock contention, no overdraft, no lost update.",
		HotWallets:  3, HotOpenMinor: 10000, Rate: 60, Duration: time.Minute, Warmup: 5 * time.Second,
		Mix: map[string]float64{TaskHot: 100},
	},
	"duplicates": {
		Description: "Each BET sent 10 times (6 HTTP over the 3 instances, 4 SQS) within 300 ms, plus 2 conflict probes: exactly one effect.",
		Wallets:     200, OpeningMinor: 100000, Rate: 60, Duration: time.Minute, Warmup: 5 * time.Second,
		Mix:       map[string]float64{TaskDuplicate: 100},
		DupSpread: 300 * time.Millisecond,
	},
	"reversal-first": {
		Description: "REFUND/ROLLBACK sent before its BET (202 PENDING_REFERENCE), BET 0-2 s later: the reversal must resolve to PROCESSED.",
		Wallets:     200, OpeningMinor: 100000, Rate: 20, Duration: time.Minute, Warmup: 5 * time.Second,
		Mix:      map[string]float64{TaskReversalFirst: 100},
		MaxDelay: 2 * time.Second,
	},
	"ramp": {
		Description: "Steady mix with the rate rising in steps, to find the knee (last step with p99 < 500 ms and errors < 1%).",
		Wallets:     1000, OpeningMinor: 100000, Rate: 20, Duration: 210 * time.Second, Warmup: 0,
		Mix:      map[string]float64{TaskRoundHTTP: 80, TaskRoundSQS: 20},
		RampStep: 20, RampEvery: 30 * time.Second, RampMax: 140,
	},
}

func init() {
	for name, p := range profiles {
		p.Name = name
		if p.MaxInFlight == 0 {
			p.MaxInFlight = 4000
		}
		if p.DrainTimeout == 0 {
			p.DrainTimeout = 90 * time.Second
		}
		if p.ResolveWithin == 0 {
			p.ResolveWithin = 60 * time.Second
		}
		if p.DupSpread == 0 {
			p.DupSpread = 300 * time.Millisecond
		}
		if p.MaxDelay == 0 {
			p.MaxDelay = 2 * time.Second
		}
		profiles[name] = p
	}
}

func Get(name string) (Profile, error) {
	p, ok := profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("unknown scenario %q (known: %s)", name, strings.Join(Names(), ", "))
	}
	return p, nil
}

func Names() []string {
	var out []string
	for n := range profiles {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// OpsPerTask is the weighted mean of sends per task for a mix.
func OpsPerTask(mix map[string]float64) float64 {
	var w, ops float64
	for t, weight := range mix {
		w += weight
		ops += weight * opsPerTask[t]
	}
	if w == 0 {
		return 1
	}
	return ops / w
}

// ParseMix reads "round-http=80,round-sqs=20".
func ParseMix(s string) (map[string]float64, error) {
	out := map[string]float64{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("mix entry %q is not task=weight", part)
		}
		if _, known := opsPerTask[k]; !known {
			return nil, fmt.Errorf("unknown task type %q", k)
		}
		var w float64
		if _, err := fmt.Sscanf(v, "%g", &w); err != nil || w < 0 {
			return nil, fmt.Errorf("invalid weight %q", v)
		}
		out[k] = w
	}
	return out, nil
}

// WithoutSQS folds SQS tasks into their HTTP counterpart (for environments
// without a broker, such as the unit test fake).
func WithoutSQS(mix map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range mix {
		if k == TaskRoundSQS {
			k = TaskRoundHTTP
		}
		out[k] += v
	}
	return out
}
