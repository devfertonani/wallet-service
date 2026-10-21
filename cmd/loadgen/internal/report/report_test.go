package report

import (
	"strings"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
)

func histOf(ms ...int) *hist.Histogram {
	h := hist.New()
	for _, v := range ms {
		h.Record(time.Duration(v) * time.Millisecond)
	}
	return h
}

func TestKneeIsLastStepBeforeFirstViolation(t *testing.T) {
	dir := t.TempDir()
	run := result.Run{Scenario: "ramp", Valid: true, OpsMeasured: 300, MeasuredSec: 30,
		Series: []hist.Series{
			{Key: hist.Key{Channel: "http", Kind: "BET", Outcome: "PROCESSED"}, Histogram: histOf(10, 20, 30)},
			{Key: hist.Key{Channel: "http", Kind: "BET", Outcome: "error"}, Histogram: histOf(5000)},
			{Key: hist.Key{Channel: "http", Kind: "BET", Outcome: "REJECTED:INSUFFICIENT_FUNDS"}, Histogram: histOf(15)},
		},
		Steps: []result.Step{
			{Step: 0, TargetOpsSec: 100, Ops: 100, Latency: histOf(10, 20)},
			{Step: 1, TargetOpsSec: 200, Ops: 100, Errors: 0, Latency: histOf(30, 40)},
			{Step: 2, TargetOpsSec: 300, Ops: 100, Errors: 5, Latency: histOf(30)}, // 5% errors
			{Step: 3, TargetOpsSec: 400, Ops: 100, Latency: histOf(10)},            // after the knee: ignored
		}}
	if err := result.Write(dir, result.RunFile, run); err != nil {
		t.Fatal(err)
	}
	if err := result.Write(dir, result.VerifyFile, result.Verify{Pass: true}); err != nil {
		t.Fatal(err)
	}
	s, err := Write(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.KneeOpsSec != 200 || s.Steps[3].WithinKnee {
		t.Fatalf("knee = %v, steps %+v", s.KneeOpsSec, s.Steps)
	}
	if s.Verdict != "PASS" || s.Errors != 1 || s.Rejected["INSUFFICIENT_FUNDS"] != 1 || s.OpsPerSec != 10 {
		t.Fatalf("summary: %+v", s)
	}
	md := Markdown(s, "", "")
	if !strings.Contains(md, "Knee: **200.0 op/s**") {
		t.Fatalf("knee missing from markdown:\n%s", md)
	}
}

func TestVerdict(t *testing.T) {
	for _, c := range []struct {
		valid, pass, verified bool
		want                  string
	}{
		{true, true, true, "PASS"}, {false, true, true, "INVALID"}, {true, false, true, "FAIL"}, {true, true, false, "FAIL"},
	} {
		dir := t.TempDir()
		_ = result.Write(dir, result.RunFile, result.Run{Valid: c.valid, InvalidReasons: []string{"x"}})
		if c.verified {
			_ = result.Write(dir, result.VerifyFile, result.Verify{Pass: c.pass})
		}
		s, err := Build(dir)
		if err != nil {
			t.Fatal(err)
		}
		if s.Verdict != c.want {
			t.Errorf("valid=%v pass=%v verified=%v: verdict %s, want %s", c.valid, c.pass, c.verified, s.Verdict, c.want)
		}
	}
}
