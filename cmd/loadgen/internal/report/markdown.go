package report

import (
	"fmt"
	"sort"
	"strings"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/baseline"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
)

func f1(v float64) string { return fmt.Sprintf("%.1f", v) }
func f2(v float64) string { return fmt.Sprintf("%.2f", v) }

func latencyTable(b *strings.Builder, rows []Row, withOutcome bool) {
	if withOutcome {
		b.WriteString("| Channel | Kind | Outcome | N | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) |\n| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	} else {
		b.WriteString("| Channel | Kind | N | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) |\n| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	}
	for _, r := range rows {
		if withOutcome {
			fmt.Fprintf(b, "| %s | %s | %s | %d | %s | %s | %s | %s | %s |\n", r.Channel, r.Kind, r.Outcome, r.Count,
				f1(r.P50Ms), f1(r.P95Ms), f1(r.P99Ms), f1(r.P999Ms), f1(r.MaxMs))
		} else {
			fmt.Fprintf(b, "| %s | %s | %d | %s | %s | %s | %s | %s |\n", r.Channel, r.Kind, r.Count,
				f1(r.P50Ms), f1(r.P95Ms), f1(r.P99Ms), f1(r.P999Ms), f1(r.MaxMs))
		}
	}
	b.WriteString("\n")
}

func summaryLine(b *strings.Builder, label string, s *hist.Summary, unit string) {
	if s == nil {
		return
	}
	fmt.Fprintf(b, "| %s | %d | %s | %s | %s | %s |\n", label, s.Count, f1(s.P50Ms)+unit, f1(s.P95Ms)+unit, f1(s.P99Ms)+unit, f1(s.MaxMs)+unit)
}

// Markdown renders report.md. Sentences are one per line on purpose, so the
// file diffs well.
func Markdown(s *Summary, pgss, events string) string {
	if s.Run == nil && s.Baseline != nil {
		return baselineMarkdown(s)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Load report: %s\n\n", s.Scenario)
	fmt.Fprintf(&b, "Result: **%s**.\n", s.Verdict)
	fmt.Fprintf(&b, "Run `%s`", s.RunID)
	if s.Run != nil {
		fmt.Fprintf(&b, ", from %s to %s (UTC)", s.Run.StartedAt.UTC().Format("2006-01-02 15:04:05"), s.Run.LoadEndedAt.UTC().Format("15:04:05"))
	}
	b.WriteString(".\n")
	switch s.Verdict {
	case "PASS":
		b.WriteString("All invariants and the cross-check against the journal passed, and the generator kept its schedule.\n")
	case "INVALID":
		b.WriteString("The invariants passed, but the generator did not keep its schedule: the latency and throughput numbers are not reliable.\n")
	default:
		b.WriteString("At least one check failed; see the reasons below.\n")
	}
	if len(s.Reasons) > 0 {
		b.WriteString("\nReasons:\n\n")
		for _, r := range s.Reasons {
			fmt.Fprintf(&b, "- %s\n", r)
		}
	}

	b.WriteString("\n## How to reproduce\n\n")
	if s.Repro != "" {
		b.WriteString("From the repository root:\n\n```sh\n" + s.Repro + "\n```\n\n")
	} else {
		b.WriteString("Full orchestration: `loadtest/run.sh " + s.Scenario + "` (see `loadtest/README.md`).\n\n")
	}
	if len(s.Command) > 0 {
		b.WriteString("Generator command for this step:\n\n```sh\n" + strings.Join(s.Command, " ") + "\n```\n")
	}

	b.WriteString("\n## Environment\n\n")
	if s.Env != "" {
		b.WriteString("```text\n" + s.Env + "\n```\n\n")
	} else {
		b.WriteString("Environment fingerprint not collected (`env.txt` missing; `run.sh` generates it).\n\n")
	}
	if s.Run != nil {
		b.WriteString("Scenario parameters:\n\n| Parameter | Value |\n| --- | --- |\n")
		keys := make([]string, 0, len(s.Run.Params))
		for k := range s.Run.Params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "| %s | %v |\n", k, s.Run.Params[k])
		}
		fmt.Fprintf(&b, "| instances | %s |\n", strings.Join(s.Run.Targets, ", "))
	}

	b.WriteString("\n## Methodology\n\n")
	b.WriteString("Open model: arrivals follow a constant rate, independent of the responses, and latency is measured from the scheduled instant (no coordinated omission).\n")
	b.WriteString("Client latency includes retries: `503` responses and timeouts are retried with the same `Idempotency-Key`, with exponential backoff and jitter.\n")
	b.WriteString("Operations scheduled during the warm-up are left out of the statistics, but they are journaled and verified.\n")
	b.WriteString("Within a round, the first operation is measured from its scheduled instant and the next one from the moment the provider would send it (after the bet response).\n")
	b.WriteString("On SQS, client latency is only the send to the broker; the end-to-end latency (scheduled until the row is written) comes from the database.\n")
	b.WriteString("Postgres runs with `fsync` and `synchronous_commit` on.\n")
	b.WriteString("Generator, service and dependencies share the same host; the numbers are for comparing versions on the same machine, not a production capacity figure.\n")

	b.WriteString("\n## Throughput\n\n")
	fmt.Fprintf(&b, "| Metric | Value |\n| --- | ---: |\n| measured operations | %d |\n| measured window (s) | %s |\n| throughput (op/s) | %s |\n",
		s.OpsTotal, f1(s.MeasuredS), f1(s.OpsPerSec))
	for _, r := range s.ByChannel {
		if s.MeasuredS > 0 {
			fmt.Fprintf(&b, "| throughput %s (op/s) | %s |\n", r.Channel, f1(float64(r.Count)/s.MeasuredS))
		}
	}
	if s.Seed != nil {
		fmt.Fprintf(&b, "| wallets opened (POST /wallets) | %d in %ss (%s/s) |\n", s.Seed.Wallets, f1(s.Seed.Seconds), f1(s.Seed.PerSecond))
	}

	b.WriteString("\n## Client latency\n\nBy channel:\n\n")
	latencyTable(&b, s.ByChannel, false)
	b.WriteString("By channel and kind:\n\n")
	latencyTable(&b, s.ByKind, false)
	b.WriteString("By channel, kind and outcome:\n\n")
	latencyTable(&b, s.Series, true)
	if s.SQSE2E != nil || s.Resolution != nil || s.EventDelivery != nil || (s.Seed != nil && s.Seed.Latency != nil) {
		b.WriteString("Other latencies:\n\n| Measure | N | p50 | p95 | p99 | max |\n| --- | ---: | ---: | ---: | ---: | ---: |\n")
		summaryLine(&b, "SQS end to end (scheduled until the transaction is written)", s.SQSE2E, " ms")
		summaryLine(&b, "early reversal: BET answered until the reversal is resolved", s.Resolution, " ms")
		summaryLine(&b, "event delivered to the consumer (occurredAt until received on the audit queue)", s.EventDelivery, " ms")
		if s.Seed != nil && s.Seed.Latency != nil {
			x := s.Seed.Latency.Summary()
			summaryLine(&b, "wallet opening (POST /wallets)", &x, " ms")
		}
		b.WriteString("\n")
	}

	b.WriteString("## Errors and conflicts\n\n| Measure | Value |\n| --- | ---: |\n")
	fmt.Fprintf(&b, "| final errors (no response, final 5xx or unexpected 4xx) | %d (%s%%) |\n", s.Errors, f2(100*s.ErrorRatio))
	if s.Run != nil {
		fmt.Fprintf(&b, "| HTTP retries (extra attempts) | %d |\n| retried 503 responses | %d |\n", s.Run.HTTPRetries, s.Run.HTTP503)
		fmt.Fprintf(&b, "| SQS batches sent / batch retries | %d / %d |\n", s.Run.SQSBatches, s.Run.SQSRetries)
	}
	fmt.Fprintf(&b, "| HTTP idempotent replays (`idempotentReplay: true`) | %d |\n| 409 responses | %d |\n", s.Replays, s.Conflicts)
	codes := make([]string, 0, len(s.Rejected))
	for c := range s.Rejected {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	for _, c := range codes {
		fmt.Fprintf(&b, "| 422 `%s` | %d |\n", c, s.Rejected[c])
	}
	fmt.Fprintf(&b, "| max sessions waiting on a lock (sampled every 2 s) | %d |\n", s.MaxLocks)
	if v := s.Verify; v != nil {
		st := v.Cross.Stats
		fmt.Fprintf(&b, "| logical operations / copies / 409 probes | %d / %d / %d |\n", st.LogicalOps, st.Copies, st.Probes)
		fmt.Fprintf(&b, "| outcome unknown to the client, applied / not applied | %d / %d |\n", st.UnknownApplied, st.UnknownNotApplied)
		keys := make([]string, 0, len(st.ByFinalStatus))
		for k := range st.ByFinalStatus {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "| final status in the database `%s` | %d |\n", k, st.ByFinalStatus[k])
		}
	}

	b.WriteString("\n## Outbox\n\n")
	if v := s.Verify; v != nil {
		o := v.Outbox
		b.WriteString("Publication delay (`published_at − occurred_at`), computed in SQL over every event in the run window.\n\n")
		fmt.Fprintf(&b, "| Measure | Value |\n| --- | ---: |\n| events | %d |\n| unpublished | %d |\n| p50 (s) | %s |\n| p95 (s) | %s |\n| p99 (s) | %s |\n| max (s) | %s |\n",
			o.Events, o.Unpublished, f2(o.P50Sec), f2(o.P95Sec), f2(o.P99Sec), f2(o.MaxSec))
		fmt.Fprintf(&b, "| events produced per second during the load | %s |\n| events published per second during the load | %s |\n", f1(o.ProducedPerSec), f1(o.PublishedPerSec))
		fmt.Fprintf(&b, "| largest backlog during the load | %d |\n", s.MaxBacklog)
		fmt.Fprintf(&b, "| outbox / input queue backlog at the end of the load | %d / %d |\n", o.BacklogAtStop, o.QueueAtStop)
		if o.DrainPerSec > 0 || o.QueueDrainPerSec > 0 {
			fmt.Fprintf(&b, "| outbox / input queue drain (per second) | %s / %s |\n", f1(o.DrainPerSec), f1(o.QueueDrainPerSec))
		}
		fmt.Fprintf(&b, "| events consumed from the audit queue | %d |\n", s.AuditDrained)
		if o.Drained {
			fmt.Fprintf(&b, "| drain after the end of the load (s) | %s |\n", f1(o.DrainSec))
		} else {
			fmt.Fprintf(&b, "| drain | did not drain: %s |\n", o.DrainDetails)
		}
	} else {
		b.WriteString("No verification, no outbox statistics.\n")
	}

	if len(s.Steps) > 0 {
		fmt.Fprintf(&b, "\n## Ramp\n\nKnee: last step with p99 < %d ms and errors < %.0f%%, before the first step that breaks either.\n\n", KneeP99Ms, KneeErrorRatio*100)
		b.WriteString("| Step | Target (op/s) | Operations | Errors | p50 (ms) | p99 (ms) | Largest outbox backlog | MiniStack CPU | Within |\n| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
		for _, st := range s.Steps {
			ok := "yes"
			if !st.WithinKnee {
				ok = "no"
			}
			fmt.Fprintf(&b, "| %d | %s | %d | %d | %s | %s | %d | %s%% | %s |\n", st.Step, f1(st.TargetOpsSec), st.Ops, st.Errors, f1(st.P50Ms), f1(st.P99Ms), st.MaxBacklog, f1(st.MiniStackCPU), ok)
		}
		if s.KneeOpsSec > 0 {
			fmt.Fprintf(&b, "\nKnee: **%s op/s**.\n", f1(s.KneeOpsSec))
		} else {
			b.WriteString("\nNo step met the criterion.\n")
		}
	}

	b.WriteString("\n## Verification\n\n")
	if v := s.Verify; v != nil {
		b.WriteString("SQL invariants over the whole database (`cmd/loadgen/internal/verify/invariants.sql`), in one `REPEATABLE READ READ ONLY` transaction:\n\n| Invariant | Description | Violations |\n| --- | --- | ---: |\n")
		for _, c := range v.Checks {
			n := fmt.Sprint(c.Violations)
			if c.Error != "" {
				n = "error: " + c.Error
			}
			fmt.Fprintf(&b, "| `%s` | %s | %s |\n", c.Name, c.Description, n)
		}
		for _, c := range v.Checks {
			if len(c.Samples) > 0 {
				fmt.Fprintf(&b, "\nSamples of `%s`:\n\n```text\n%s\n```\n", c.Name, strings.Join(c.Samples, "\n"))
			}
		}
		fmt.Fprintf(&b, "\nCross-check against the journal (independent oracle): %d wallets checked, %d violations.\n", v.Cross.Stats.WalletsChecked, len(v.Cross.Violations))
		if len(v.Cross.Violations) > 0 {
			b.WriteString("\n| Check | Detail |\n| --- | --- |\n")
			for _, x := range v.Cross.Violations {
				fmt.Fprintf(&b, "| `%s` | %s |\n", x.Check, x.Detail)
			}
		}
		fmt.Fprintf(&b, "\nReconciliation through the API: %d wallets, %d inconsistent or failed.\n", v.Reconciliation.Sampled, v.Reconciliation.Failed)
		fmt.Fprintf(&b, "Lookup by `(providerId, externalTransactionId)`: %d transactions, %d differing from the database.\n", v.Lookups.Sampled, v.Lookups.Failed)
		for _, f := range append(v.Reconciliation.Failures, v.Lookups.Failures...) {
			fmt.Fprintf(&b, "- %s\n", f)
		}
		fmt.Fprintf(&b, "DLQ: %d messages after the run (change %d).\n", v.DLQAfter, v.DLQDelta)
	} else {
		b.WriteString("Not run.\n")
	}

	cpuSection(&b, s.CPU, true)
	if s.Baseline != nil {
		baselineSection(&b, s.Baseline)
	}

	if r := s.Run; r != nil {
		b.WriteString("\n## Generator\n\n| Measure | Value |\n| --- | ---: |\n")
		fmt.Fprintf(&b, "| scheduled tasks | %d |\n| dropped tasks (generator saturated) | %d |\n| tasks abandoned at the end | %d |\n",
			r.TasksScheduled, r.TasksDropped, r.TasksAbandoned)
		if r.DispatchLag != nil {
			l := r.DispatchLag.Summary()
			fmt.Fprintf(&b, "| dispatch lag p99 / max (ms) | %s / %s |\n", f1(l.P99Ms), f1(l.MaxMs))
		}
		fmt.Fprintf(&b, "| journal lines | %d |\n", r.OpsJournaled)
		names := make([]string, 0, len(r.TokenFetches))
		for k := range r.TokenFetches {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			fmt.Fprintf(&b, "| tokens requested from Keycloak by `%s` | %d |\n", k, r.TokenFetches[k])
		}
	}
	if events != "" {
		b.WriteString("\n## Chaos events\n\n```text\n" + events + "\n```\n")
	}
	if pgss != "" {
		b.WriteString("\n## Most expensive queries (pg_stat_statements)\n\n```text\n" + pgss + "\n```\n")
	}
	return b.String()
}

func cpuSection(b *strings.Builder, rows []CPURow, withDrain bool) {
	if len(rows) == 0 {
		return
	}
	b.WriteString("\n## CPU and memory per service\n\n")
	b.WriteString("`docker stats` samples every ~2 s; 100% is one full core.\n")
	b.WriteString("A single-threaded service near 100% (the MiniStack) is a bottleneck even with the machine idle; with `LIMITS=1`, the ceiling of each capped service is its CPU limit (0.25 core = 25%), recorded in `env.txt`.\n")
	b.WriteString("Memory: peak during the run against the container limit (without a limit, Docker shows the VM memory).\n\n")
	if !withDrain {
		b.WriteString("| Service | Mean | Max | Max memory / limit (MiB) |\n| --- | ---: | ---: | ---: |\n")
		for _, r := range rows {
			fmt.Fprintf(b, "| %s | %s%% | %s%% | %s |\n", r.Service, f1(r.LoadMean), f1(r.LoadMax), memCell(r))
		}
		return
	}
	b.WriteString("| Service | Load: mean | Load: max | Drain and verification: mean | Drain and verification: max | Max memory / limit (MiB) |\n| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range rows {
		fmt.Fprintf(b, "| %s | %s%% | %s%% | %s%% | %s%% | %s |\n", r.Service, f1(r.LoadMean), f1(r.LoadMax), f1(r.DrainMean), f1(r.DrainMax), memCell(r))
	}
}

func memCell(r CPURow) string {
	if r.MemLimitMiB == 0 {
		return "—"
	}
	return fmt.Sprintf("%s / %s", f1(r.MemMaxMiB), f1(r.MemLimitMiB))
}

func baselineSection(b *strings.Builder, bl *baseline.Result) {
	b.WriteString("\n## Emulator ceiling (baseline)\n\n")
	fmt.Fprintf(b, "MiniStack SQS and SNS measured alone, without the service: %d parallel callers, %d distinct `MessageGroupId`s, %s per phase.\n", bl.Workers, bl.Groups, bl.Duration)
	b.WriteString("The first and last second of each phase show how the cost per operation grows with the queue depth.\n\n")
	b.WriteString("| Phase | Description | Messages | msg/s (mean) | msg/s in the 1st s | msg/s in the last s | call p99 (ms) | Errors |\n| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, p := range bl.Phases {
		firstS, lastS := int64(0), int64(0)
		if n := len(p.Timeline); n > 0 {
			firstS = p.Timeline[0]
			// the last bucket is partial; use the one before when possible
			lastS = p.Timeline[n-1]
			if n > 2 {
				lastS = p.Timeline[n-2]
			}
		}
		p99 := 0.0
		if p.CallLatency != nil {
			p99 = p.CallLatency.Summary().P99Ms
		}
		fmt.Fprintf(b, "| `%s` | %s | %d | %s | %d | %d | %s | %d |\n", p.Name, p.Description, p.Messages, f1(p.PerSec), firstS, lastS, f1(p99), p.Errors)
	}
	fmt.Fprintf(b, "\nSNS fan-out to `wallet-events-audit.fifo`: %d of %d events delivered, %s s after the last publish.\n", bl.FanOutDelivered, bl.FanOutPublished, f1(bl.FanOutSeconds))
}

func baselineMarkdown(s *Summary) string {
	var b strings.Builder
	b.WriteString("# Emulator ceiling (baseline)\n\n")
	if s.Baseline != nil {
		fmt.Fprintf(&b, "Measured from %s to %s (UTC).\n", s.Baseline.StartedAt.UTC().Format("2006-01-02 15:04:05"), s.Baseline.FinishedAt.UTC().Format("15:04:05"))
	}
	b.WriteString("Measures only the MiniStack, without the service: SQS send and receive on a scratch FIFO queue and SNS publish on the real topic, with the fan-out to the audit queue (emptied at the end).\n")
	if s.Repro != "" {
		b.WriteString("\n```sh\n" + s.Repro + "\n```\n")
	}
	if s.Env != "" {
		b.WriteString("\n## Environment\n\n```text\n" + s.Env + "\n```\n")
	}
	baselineSection(&b, s.Baseline)
	cpuSection(&b, s.CPU, false)
	return b.String()
}
