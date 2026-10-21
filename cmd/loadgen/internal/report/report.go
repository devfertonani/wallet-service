// Package report merges seed.json, run.json and verify.json into
// summary.json and a human readable report.md (pt-BR).
package report

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/baseline"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
)

// Knee criteria of the ramp scenario.
const (
	KneeP99Ms      = 500
	KneeErrorRatio = 0.01
)

type Row struct {
	Channel string `json:"channel"`
	Kind    string `json:"kind"`
	Outcome string `json:"outcome"`
	hist.Summary
}

type StepRow struct {
	Step         int     `json:"step"`
	TargetOpsSec float64 `json:"targetOpsPerSec"`
	Ops          int64   `json:"ops"`
	Errors       int64   `json:"errors"`
	ErrorRatio   float64 `json:"errorRatio"`
	P50Ms        float64 `json:"p50Ms"`
	P99Ms        float64 `json:"p99Ms"`
	WithinKnee   bool    `json:"withinKnee"`
	// Evidence of where the time went in this step.
	MaxBacklog   int64   `json:"maxOutboxBacklog"`
	MiniStackCPU float64 `json:"ministackCpuPct"`
}

type Summary struct {
	Verdict    string           `json:"verdict"` // PASS, FAIL or INVALID
	Pass       bool             `json:"pass"`
	Valid      bool             `json:"valid"`
	Reasons    []string         `json:"reasons"`
	RunID      string           `json:"runId"`
	Scenario   string           `json:"scenario"`
	Command    []string         `json:"command"`
	Repro      string           `json:"reproduce,omitempty"`
	OpsPerSec  float64          `json:"opsPerSec"`
	MeasuredS  float64          `json:"measuredSeconds"`
	OpsTotal   int64            `json:"opsMeasured"`
	ByChannel  []Row            `json:"byChannel"`
	ByKind     []Row            `json:"byChannelAndKind"`
	Series     []Row            `json:"series"`
	Errors     int64            `json:"errors"`
	ErrorRatio float64          `json:"errorRatio"`
	Replays    int64            `json:"httpReplays"`
	Conflicts  int64            `json:"http409"`
	Rejected   map[string]int64 `json:"rejectedByFailureCode"`
	SQSE2E     *hist.Summary    `json:"sqsEndToEnd,omitempty"`
	Resolution *hist.Summary    `json:"reversalResolution,omitempty"`
	Steps      []StepRow        `json:"steps,omitempty"`
	KneeOpsSec float64          `json:"kneeOpsPerSec,omitempty"`
	MaxBacklog int64            `json:"maxOutboxBacklog"`
	MaxLocks   int64            `json:"maxLockWaits"`

	CPU           []CPURow         `json:"cpu,omitempty"`
	Baseline      *baseline.Result `json:"baseline,omitempty"`
	EventDelivery *hist.Summary    `json:"eventDelivery,omitempty"`
	AuditDrained  int64            `json:"auditDrained"`

	Seed   *result.Seed   `json:"seed,omitempty"`
	Run    *result.Run    `json:"-"`
	Verify *result.Verify `json:"verify,omitempty"`
	Env    string         `json:"env,omitempty"`
}

func isError(outcome string) bool {
	return outcome == "error" || outcome == "ERROR" || strings.HasPrefix(outcome, "http_5") ||
		(strings.HasPrefix(outcome, "http_4") && !strings.HasPrefix(outcome, "http_409"))
}

// Build reads the run directory and computes the summary.
func Build(dir string) (*Summary, error) {
	var bl baseline.Result
	hasBaseline := result.Read(dir, baseline.File, &bl) == nil
	var run result.Run
	if err := result.Read(dir, result.RunFile, &run); err != nil {
		if hasBaseline {
			return baselineOnly(dir, &bl), nil
		}
		return nil, fmt.Errorf("read %s: %w", result.RunFile, err)
	}
	s := &Summary{RunID: run.RunID, Scenario: run.Scenario, Command: run.Command, Run: &run,
		Valid: run.Valid, MeasuredS: run.MeasuredSec, OpsTotal: run.OpsMeasured, Rejected: map[string]int64{}}
	var seed result.Seed
	if result.Read(dir, result.SeedFile, &seed) == nil {
		s.Seed = &seed
	}
	var ver result.Verify
	if err := result.Read(dir, result.VerifyFile, &ver); err == nil {
		s.Verify = &ver
		s.Pass = ver.Pass
		s.Reasons = append(s.Reasons, ver.FailReasons...)
	} else {
		s.Reasons = append(s.Reasons, "verification not run ("+result.VerifyFile+" missing)")
	}
	s.Repro = readOptional(dir, result.CommandFile)
	if b, err := os.ReadFile(filepath.Join(dir, result.EnvFile)); err == nil {
		s.Env = strings.TrimSpace(string(b))
	}
	s.Reasons = append(s.Reasons, run.InvalidReasons...)
	switch {
	case !s.Pass:
		s.Verdict = "FAIL"
	case !s.Valid:
		s.Verdict = "INVALID"
	default:
		s.Verdict = "PASS"
	}
	if run.MeasuredSec > 0 {
		s.OpsPerSec = float64(run.OpsMeasured) / run.MeasuredSec
	}

	chans := map[string]*hist.Histogram{}
	kinds := map[[2]string]*hist.Histogram{}
	for _, se := range run.Series {
		h := se.Histogram
		s.Series = append(s.Series, Row{se.Channel, se.Kind, se.Outcome, h.Summary()})
		if chans[se.Channel] == nil {
			chans[se.Channel] = hist.New()
		}
		chans[se.Channel].Merge(h)
		k := [2]string{se.Channel, se.Kind}
		if kinds[k] == nil {
			kinds[k] = hist.New()
		}
		kinds[k].Merge(h)
		n := int64(h.Count())
		switch {
		case isError(se.Outcome):
			s.Errors += n
		case strings.HasSuffix(se.Outcome, "/replay"):
			s.Replays += n
		case strings.HasPrefix(se.Outcome, "http_409"):
			s.Conflicts += n
		case strings.HasPrefix(se.Outcome, "REJECTED:"):
			s.Rejected[strings.TrimPrefix(se.Outcome, "REJECTED:")] += n
		}
	}
	if run.OpsMeasured > 0 {
		s.ErrorRatio = float64(s.Errors) / float64(run.OpsMeasured)
	}
	for c, h := range chans {
		s.ByChannel = append(s.ByChannel, Row{Channel: c, Kind: "*", Outcome: "*", Summary: h.Summary()})
	}
	for k, h := range kinds {
		s.ByKind = append(s.ByKind, Row{Channel: k[0], Kind: k[1], Outcome: "*", Summary: h.Summary()})
	}
	sortRows(s.ByChannel)
	sortRows(s.ByKind)

	if ver.SQSEndToEnd != nil && ver.SQSEndToEnd.Count() > 0 {
		x := ver.SQSEndToEnd.Summary()
		s.SQSE2E = &x
	}
	if run.Resolution != nil && run.Resolution.Count() > 0 {
		x := run.Resolution.Summary()
		s.Resolution = &x
	}
	for _, d := range run.DBSamples {
		s.MaxBacklog = max(s.MaxBacklog, d.OutboxBacklog)
		s.MaxLocks = max(s.MaxLocks, d.LockWaits)
	}

	if hasBaseline {
		s.Baseline = &bl
	}
	s.CPU = readCPU(dir, run.StartedAt, run.LoadEndedAt)
	delivery := hist.New()
	delivery.Merge(run.EventDelivery)
	delivery.Merge(ver.EventDelivery)
	s.AuditDrained = run.AuditDrained + ver.AuditDrained
	if delivery.Count() > 0 {
		x := delivery.Summary()
		s.EventDelivery = &x
	}

	broken := false
	for _, st := range run.Steps {
		r := StepRow{Step: st.Step, TargetOpsSec: st.TargetOpsSec, Ops: st.Ops, Errors: st.Errors}
		if st.Ops > 0 {
			r.ErrorRatio = float64(st.Errors) / float64(st.Ops)
		}
		if st.Latency != nil {
			r.P50Ms, r.P99Ms = st.Latency.Summary().P50Ms, st.Latency.Summary().P99Ms
		}
		if run.StepSec > 0 {
			from := run.StartedAt.Add(time.Duration(float64(st.Step) * run.StepSec * float64(time.Second)))
			to := from.Add(time.Duration(run.StepSec * float64(time.Second)))
			for _, d := range run.DBSamples {
				if at := run.StartedAt.Add(time.Duration(d.OffsetSec * float64(time.Second))); !at.Before(from) && at.Before(to) {
					r.MaxBacklog = max(r.MaxBacklog, d.OutboxBacklog)
				}
			}
			r.MiniStackCPU = meanCPU(dir, "ministack", from, to)
		}
		r.WithinKnee = !broken && r.P99Ms < KneeP99Ms && r.ErrorRatio < KneeErrorRatio
		if r.WithinKnee {
			s.KneeOpsSec = r.TargetOpsSec
		} else {
			broken = true
		}
		s.Steps = append(s.Steps, r)
	}
	return s, nil
}

func sortRows(r []Row) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].Channel != r[j].Channel {
			return r[i].Channel < r[j].Channel
		}
		return r[i].Kind < r[j].Kind
	})
}

// Write builds the summary and writes summary.json and report.md.
func Write(dir string) (*Summary, error) {
	s, err := Build(dir)
	if err != nil {
		return nil, err
	}
	if err := result.Write(dir, result.SummaryFile, s); err != nil {
		return nil, err
	}
	md := Markdown(s, readOptional(dir, result.PGSSFile), readOptional(dir, result.EventsFile))
	return s, os.WriteFile(filepath.Join(dir, result.ReportFile), []byte(md), 0o644)
}

func readOptional(dir, name string) string {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// baselineOnly is the summary of a "loadtest/run.sh baseline" directory.
func baselineOnly(dir string, bl *baseline.Result) *Summary {
	s := &Summary{Verdict: "BASELINE", Pass: true, Valid: true, Scenario: "baseline", Baseline: bl,
		Repro: readOptional(dir, result.CommandFile), Env: readOptional(dir, result.EnvFile)}
	s.CPU = readCPU(dir, bl.StartedAt, bl.FinishedAt)
	return s
}
