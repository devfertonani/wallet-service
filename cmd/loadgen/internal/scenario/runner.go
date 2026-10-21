package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/auth"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/sched"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/target"
	"github.com/google/uuid"
)

const gameID = "load-test-slot"

// Runner executes one scenario run. Fill the exported fields, then call Run.
type Runner struct {
	Profile   Profile
	RunID     string
	Currency  string
	Wallets   []journal.Wallet
	Hot       []journal.Wallet
	HTTP      *target.HTTP
	Providers []*auth.Source // provider id = client id
	Producer  *target.Producer
	Journal   *journal.Writer
	Metrics   *Metrics
	// Sampler, when set, is called every 2 s during the run (DB and queue
	// gauges for the report).
	Sampler func(ctx context.Context) (result.DBSample, error)
	Logf    func(format string, args ...any)

	rec        *hist.Recorder
	lag        *hist.Histogram
	resolution *hist.Histogram
	mu         sync.Mutex
	steps      map[int]*result.Step
	samples    []result.DBSample

	seq       atomic.Int64 // external transaction ids
	jseq      atomic.Int64 // journal line numbers
	retried   atomic.Int64 // 503 answers retried
	tasks     atomic.Int64
	dropped   atomic.Int64
	running   atomic.Int64
	measured  atomic.Int64
	start     time.Time
	warmupEnd time.Time
}

func (r *Runner) logf(format string, args ...any) {
	if r.Logf != nil {
		r.Logf(format, args...)
	}
}

func (r *Runner) taskRate(e time.Duration) float64 {
	return r.opsRate(e) / OpsPerTask(r.Profile.Mix)
}

func (r *Runner) opsRate(e time.Duration) float64 {
	p := r.Profile
	if p.RampEvery > 0 {
		return sched.Steps(p.Rate, p.RampStep, p.RampMax, p.RampEvery)(e)
	}
	return p.Rate
}

// Run executes the scenario and returns the run summary (without Command
// and Params, which the caller fills).
func (r *Runner) Run(ctx context.Context) result.Run {
	r.rec = hist.NewRecorder()
	r.lag = hist.New()
	r.resolution = hist.New()
	r.steps = map[int]*result.Step{}
	if r.Currency == "" {
		r.Currency = "BRL"
	}

	pick := newPicker(r.Profile.Mix)
	sem := make(chan struct{}, r.Profile.MaxInFlight)
	var wg sync.WaitGroup
	taskCtx, cancelTasks := context.WithCancel(ctx)
	defer cancelTasks()

	sampleCtx, stopSampling := context.WithCancel(ctx)
	var sampleWG sync.WaitGroup
	r.start = time.Now().Add(200 * time.Millisecond).Truncate(time.Millisecond)
	r.warmupEnd = r.start.Add(r.Profile.Warmup)
	if r.Sampler != nil {
		sampleWG.Add(1)
		go func() { defer sampleWG.Done(); r.sample(sampleCtx) }()
	}

	plan := sched.Plan{Duration: r.Profile.Duration, Rate: r.taskRate}
	r.logf("run %s: scenario %s, %.0f op/s (%.2f ops/task), %s (warmup %s)",
		r.RunID, r.Profile.Name, r.opsRate(0), OpsPerTask(r.Profile.Mix), r.Profile.Duration, r.Profile.Warmup)
	st := sched.Run(ctx, r.start, plan, func(seq int64, at time.Time, lag time.Duration) {
		r.tasks.Add(1)
		r.lag.Record(lag) // only the scheduler goroutine touches lag
		if r.Metrics != nil {
			r.Metrics.Scheduled.Inc()
			r.Metrics.TargetRate.Set(r.opsRate(at.Sub(r.start)))
		}
		select {
		case sem <- struct{}{}:
		default:
			r.dropped.Add(1)
			if r.Metrics != nil {
				r.Metrics.Dropped.Inc()
			}
			return
		}
		task := pick()
		wg.Add(1)
		r.running.Add(1)
		go func() {
			defer func() { <-sem; r.running.Add(-1); wg.Done() }()
			r.runTask(taskCtx, task, seq, at)
		}()
	})
	loadEnded := time.Now()
	r.logf("schedule done: %d tasks, %d dropped, waiting for %d in flight", st.Arrivals, r.dropped.Load(), r.running.Load())

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(r.Profile.DrainTimeout):
		r.logf("drain timeout: cancelling %d tasks", r.running.Load())
		cancelTasks()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}
	abandoned := r.running.Load()
	stopSampling()
	sampleWG.Wait()

	out := result.Run{
		RunID: r.RunID, Scenario: r.Profile.Name, Targets: r.HTTP.Targets,
		StartedAt: r.start, WarmupEnds: r.warmupEnd, LoadEndedAt: loadEnded, FinishedAt: time.Now(),
		TasksScheduled: st.Arrivals, TasksDropped: r.dropped.Load(), TasksAbandoned: abandoned,
		OpsJournaled: r.Journal.Count(), OpsMeasured: r.measured.Load(),
		DispatchLag: r.lag, Series: r.rec.Snapshot(), Resolution: r.resolution,
		TokenFetches: map[string]int64{}, HTTPRetries: r.HTTP.Retries(), HTTP503: r.retried.Load(),
	}
	if end := loadEnded; end.After(r.warmupEnd) {
		out.MeasuredSec = end.Sub(r.warmupEnd).Seconds()
	}
	for _, p := range r.Providers {
		out.TokenFetches[p.ClientID()] = p.Fetches()
	}
	if r.Producer != nil {
		out.SQSBatches, out.SQSRetries = r.Producer.Batches(), r.Producer.Retries()
	}
	r.mu.Lock()
	for _, s := range r.steps {
		out.Steps = append(out.Steps, *s)
	}
	out.DBSamples = r.samples
	r.mu.Unlock()
	sortSteps(out.Steps)

	out.Valid = true
	if out.TasksDropped > 0 {
		out.Valid = false
		out.InvalidReasons = append(out.InvalidReasons, fmt.Sprintf("generator saturated: %d tasks dropped (max in flight %d)", out.TasksDropped, r.Profile.MaxInFlight))
	}
	if p99 := r.lag.Quantile(0.99); p99 > 100*time.Millisecond {
		out.Valid = false
		out.InvalidReasons = append(out.InvalidReasons, fmt.Sprintf("generator fell behind schedule: dispatch lag p99 %s", p99))
	}
	if abandoned > 0 {
		out.Valid = false
		out.InvalidReasons = append(out.InvalidReasons, fmt.Sprintf("%d tasks still running after the drain timeout", abandoned))
	}
	return out
}

func sortSteps(s []result.Step) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].Step < s[j-1].Step; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func (r *Runner) sample(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		s, err := r.Sampler(sctx)
		cancel()
		if err != nil {
			continue
		}
		s.OffsetSec = time.Since(r.start).Seconds()
		r.mu.Lock()
		r.samples = append(r.samples, s)
		r.mu.Unlock()
		if r.Metrics != nil {
			r.Metrics.ObserveDB(s)
		}
	}
}

// newPicker draws a task type according to the mix weights.
func newPicker(mix map[string]float64) func() string {
	type entry struct {
		name string
		cum  float64
	}
	var entries []entry
	var total float64
	for _, n := range []string{TaskRoundHTTP, TaskRoundSQS, TaskHot, TaskDuplicate, TaskReversalFirst} {
		if w := mix[n]; w > 0 {
			total += w
			entries = append(entries, entry{n, total})
		}
	}
	return func() string {
		x := rand.Float64() * total
		for _, e := range entries {
			if x < e.cum {
				return e.name
			}
		}
		return entries[len(entries)-1].name
	}
}

func (r *Runner) runTask(ctx context.Context, task string, id int64, at time.Time) {
	switch task {
	case TaskRoundHTTP:
		r.round(ctx, id, at, journal.ChannelHTTP)
	case TaskRoundSQS:
		if r.Producer == nil {
			r.round(ctx, id, at, journal.ChannelHTTP)
			return
		}
		r.round(ctx, id, at, journal.ChannelSQS)
	case TaskHot:
		r.hot(ctx, id, at)
	case TaskDuplicate:
		r.duplicate(ctx, id, at)
	case TaskReversalFirst:
		r.reversalFirst(ctx, id, at)
	}
}

// ---- operations -----------------------------------------------------------

func (r *Runner) extID() string { return fmt.Sprintf("%s-%d", r.RunID, r.seq.Add(1)) }

func (r *Runner) newOp(task string, taskID int64, at time.Time, w journal.Wallet, prov *auth.Source, kind string, amount int64) journal.Op {
	ext := r.extID()
	return journal.Op{
		Task: task, TaskID: taskID, Kind: kind, Provider: prov.ClientID(), ExtID: ext,
		IdemKey: prov.ClientID() + ":" + ext, WalletID: w.ID, PlayerID: w.PlayerID,
		RoundID: fmt.Sprintf("%s-r%d", r.RunID, taskID), Amount: amount, Currency: r.Currency,
		Scheduled: at, Warmup: at.Before(r.warmupEnd), Step: sched.StepIndex(at.Sub(r.start), r.Profile.RampEvery),
	}
}

func (r *Runner) request(op journal.Op) target.WagerRequest {
	return target.WagerRequest{
		ProviderID: op.Provider, ExternalTransactionID: op.ExtID, PlayerID: op.PlayerID, WalletID: op.WalletID,
		RoundID: op.RoundID, GameID: gameID, Kind: op.Kind, Money: target.MoneyOf(op.Amount, op.Currency),
		ReferenceExternalTransactionID: op.RefExtID,
	}
}

func (r *Runner) provider(name string) *auth.Source {
	for _, p := range r.Providers {
		if p.ClientID() == name {
			return p
		}
	}
	return r.Providers[0]
}

// sendHTTP posts the operation and fills the outcome fields of op.
func (r *Runner) sendHTTP(ctx context.Context, op *journal.Op, pin int) {
	op.Channel = journal.ChannelHTTP
	body, _ := json.Marshal(r.request(*op))
	res := r.HTTP.Do(ctx, target.Request{
		Method: http.MethodPost, Path: "/wagering/transactions", Token: r.provider(op.Provider),
		Headers: map[string]string{"Idempotency-Key": op.IdemKey}, Body: body, Pin: pin,
	})
	ApplyHTTP(op, res)
	r.retried.Add(int64(res.Retried503))
	if r.Metrics != nil && res.Retried503 > 0 {
		r.Metrics.Retried503.Add(float64(res.Retried503))
	}
}

// ApplyHTTP copies an HTTP answer into the journal entry.
func ApplyHTTP(op *journal.Op, res target.Response) {
	op.Done = time.Now()
	op.Attempts, op.Target, op.HTTPStatus = res.Attempts, res.Target, res.Status
	if res.Err != nil {
		op.Status, op.Err = journal.StatusError, res.Err.Error()
		return
	}
	var wr target.WagerResponse
	_ = json.Unmarshal(res.Body, &wr)
	switch {
	case res.Status == 200 || res.Status == 202 || res.Status == 422:
		op.Status, op.FailureCode, op.TransactionID, op.Replay = wr.Status, wr.FailureCode, wr.TransactionID, wr.IdempotentReplay
		if wr.Balance != nil {
			if v, err := target.ParseMinor(wr.Balance.Amount); err == nil {
				op.BalanceMinor = &v
			}
		}
	case res.Status >= 500:
		op.Status = journal.StatusError
	default:
		op.Status = fmt.Sprintf("HTTP_%d", res.Status)
	}
	if wr.Error != nil {
		op.ErrorCode = wr.Error.Code
	}
}

// sendSQS sends the operation as a WagerTransactionRequested envelope.
// occurredAt is part of the body: a resend of the same messageId must reuse
// it, so the copies are byte-identical, as a real producer's resend would be
// (the broker deduplicates them and answers with the original's checksum).
func (r *Runner) sendSQS(ctx context.Context, op *journal.Op, messageID string, occurredAt time.Time) {
	op.Channel = journal.ChannelSQS
	op.MessageID = messageID
	data := r.request(*op)
	data.IdempotencyKey = op.IdemKey
	env := target.Envelope{MessageID: messageID, Type: "WagerTransactionRequested",
		OccurredAt: occurredAt.UTC().Format("2006-01-02T15:04:05.000Z07:00"), Data: data}
	body, _ := json.Marshal(env)
	err := r.Producer.Send(ctx, op.WalletID, messageID, string(body))
	op.Done, op.Attempts, op.Target = time.Now(), 1, "sqs"
	if err != nil {
		op.Status, op.Err = journal.StatusError, err.Error()
		return
	}
	op.Status = journal.StatusSent
}

// Outcome is the histogram label of an op.
func Outcome(op journal.Op) string {
	switch {
	case op.Channel == journal.ChannelSQS:
		return op.Status
	case op.HTTPStatus == 0:
		return "error"
	case op.HTTPStatus == 200 || op.HTTPStatus == 202:
		if op.Replay {
			return op.Status + "/replay"
		}
		return op.Status
	case op.HTTPStatus == 422:
		return "REJECTED:" + op.FailureCode
	default:
		if op.ErrorCode != "" {
			return fmt.Sprintf("http_%d:%s", op.HTTPStatus, op.ErrorCode)
		}
		return fmt.Sprintf("http_%d", op.HTTPStatus)
	}
}

// IsError is what counts as an error rate: no answer, a final 5xx, or a 4xx
// the generator did not intend (probes expect their 409).
func IsError(op journal.Op) bool {
	if op.Status == journal.StatusError {
		return true
	}
	if op.Role == journal.RoleProbe {
		return false
	}
	return op.HTTPStatus >= 400 && op.HTTPStatus != 409 && op.HTTPStatus != 422
}

func (r *Runner) record(op journal.Op) {
	op.Seq = r.jseq.Add(1)
	if err := r.Journal.Write(op); err != nil {
		r.logf("journal write: %v", err)
	}
	lat := op.Done.Sub(op.Scheduled)
	outcome := Outcome(op)
	if r.Metrics != nil {
		r.Metrics.Observe(op.Channel, op.Kind, outcome, lat)
	}
	if !op.Warmup {
		r.measured.Add(1)
		r.rec.Record(hist.Key{Channel: op.Channel, Kind: op.Kind, Outcome: outcome}, lat)
	}
	if r.Profile.RampEvery > 0 {
		r.mu.Lock()
		s, ok := r.steps[op.Step]
		if !ok {
			s = &result.Step{Step: op.Step, Latency: hist.New(),
				TargetOpsSec: r.opsRate(time.Duration(op.Step) * r.Profile.RampEvery)}
			r.steps[op.Step] = s
		}
		s.Ops++
		if IsError(op) {
			s.Errors++
		}
		s.Latency.Record(lat)
		r.mu.Unlock()
	}
}

func (r *Runner) randWallet() journal.Wallet { return r.Wallets[rand.IntN(len(r.Wallets))] }
func (r *Runner) randProvider() *auth.Source { return r.Providers[rand.IntN(len(r.Providers))] }

func between(lo, hi int64) int64 { return lo + rand.Int64N(hi-lo+1) }

// round: BET, then 45% WIN, 45% LOSS, 5% REFUND, 5% ROLLBACK of the BET.
// The first operation is measured from the scheduled arrival; the follow-up
// from the moment the provider would send it (after the BET answer).
func (r *Runner) round(ctx context.Context, id int64, at time.Time, channel string) {
	w, prov := r.randWallet(), r.randProvider()
	bet := r.newOp(TaskRoundHTTP, id, at, w, prov, "BET", between(100, 2000))
	send := func(op *journal.Op) {
		if channel == journal.ChannelSQS {
			op.Task = TaskRoundSQS
			r.sendSQS(ctx, op, uuid.NewString(), time.Now())
		} else {
			r.sendHTTP(ctx, op, -1)
		}
		r.record(*op)
	}
	send(&bet)
	if channel == journal.ChannelHTTP && bet.Status != "PROCESSED" {
		return // a rejected or failed bet ends the round
	}
	if channel == journal.ChannelSQS && bet.Status != journal.StatusSent {
		return
	}
	var settle journal.Op
	now := time.Now()
	switch x := rand.Float64(); {
	case x < 0.45:
		settle = r.newOp(TaskRoundHTTP, id, now, w, prov, "WIN", between(1, 2*bet.Amount))
	case x < 0.90:
		settle = r.newOp(TaskRoundHTTP, id, now, w, prov, "LOSS", 0)
	case x < 0.95:
		settle = r.newOp(TaskRoundHTTP, id, now, w, prov, "REFUND", bet.Amount)
		settle.RefExtID, settle.RefKind = bet.ExtID, "BET"
	default:
		settle = r.newOp(TaskRoundHTTP, id, now, w, prov, "ROLLBACK", bet.Amount)
		settle.RefExtID, settle.RefKind = bet.ExtID, "BET"
	}
	settle.Warmup, settle.Step = bet.Warmup, bet.Step
	send(&settle)
}

// hot: one BET (55%) or WIN (45%) on a hot wallet; the negative drift makes
// the balance hit zero, so INSUFFICIENT_FUNDS races are exercised too.
func (r *Runner) hot(ctx context.Context, id int64, at time.Time) {
	w := r.Hot[rand.IntN(len(r.Hot))]
	kind := "BET"
	if rand.Float64() < 0.45 {
		kind = "WIN"
	}
	op := r.newOp(TaskHot, id, at, w, r.randProvider(), kind, between(100, 1000))
	r.sendHTTP(ctx, &op, -1)
	r.record(op)
}

// duplicate: the same BET fanned out over the three instances and SQS, all
// within DupSpread, then two requests that must be refused with 409.
func (r *Runner) duplicate(ctx context.Context, id int64, at time.Time) {
	w, prov := r.randWallet(), r.randProvider()
	orig := r.newOp(TaskDuplicate, id, at, w, prov, "BET", between(100, 2000))
	sharedMsg := uuid.NewString()

	type copySpec struct {
		channel string
		pin     int
		msgID   string
	}
	var specs []copySpec
	for i := 0; i < DupHTTPCopies; i++ {
		specs = append(specs, copySpec{journal.ChannelHTTP, i % len(r.HTTP.Targets), ""})
	}
	if r.Producer != nil {
		for i := 0; i < DupSQSSameID; i++ {
			specs = append(specs, copySpec{journal.ChannelSQS, 0, sharedMsg})
		}
		for i := 0; i < DupSQSDistinct; i++ {
			specs = append(specs, copySpec{journal.ChannelSQS, 0, uuid.NewString()})
		}
	}
	var wg sync.WaitGroup
	var answered atomic.Bool
	for i, s := range specs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			delay := time.Duration(rand.Int64N(int64(r.Profile.DupSpread) + 1))
			op := orig
			if i > 0 {
				op.Role = journal.RoleCopy
			}
			op.Scheduled = at.Add(delay)
			if !sleepUntil(ctx, op.Scheduled) {
				return
			}
			if s.channel == journal.ChannelSQS {
				r.sendSQS(ctx, &op, s.msgID, at)
			} else {
				r.sendHTTP(ctx, &op, s.pin)
				if op.HTTPStatus == 200 || op.HTTPStatus == 422 {
					answered.Store(true)
				}
			}
			r.record(op)
		}()
	}
	wg.Wait()
	if !answered.Load() {
		return // without a known original the probes would be ambiguous
	}
	reused := orig
	reused.Role, reused.Expect = journal.RoleProbe, "409:IDEMPOTENCY_KEY_REUSED"
	reused.Amount++ // same key, different content
	reused.Scheduled = time.Now()
	r.sendHTTP(ctx, &reused, -1)
	r.record(reused)

	dupExt := orig
	dupExt.Role, dupExt.Expect = journal.RoleProbe, "409:DUPLICATE_EXTERNAL_TRANSACTION"
	dupExt.IdemKey = orig.IdemKey + ":other-key" // same operation, another key
	dupExt.Scheduled = time.Now()
	r.sendHTTP(ctx, &dupExt, -1)
	r.record(dupExt)
}

// reversalFirst: REFUND/ROLLBACK of a BET that was not sent yet, then the
// BET, then poll until the reversal leaves PENDING_REFERENCE.
func (r *Runner) reversalFirst(ctx context.Context, id int64, at time.Time) {
	w, prov := r.randWallet(), r.randProvider()
	amount := between(100, 2000)
	kind := "REFUND"
	if rand.IntN(2) == 0 {
		kind = "ROLLBACK"
	}
	betExt := r.extID()
	rev := r.newOp(TaskReversalFirst, id, at, w, prov, kind, amount)
	rev.RefExtID, rev.RefKind = betExt, "BET"
	r.sendHTTP(ctx, &rev, -1)
	if rev.HTTPStatus != 202 || rand.Float64() < r.Profile.OrphanRatio {
		r.record(rev)
		return
	}
	if !sleepUntil(ctx, time.Now().Add(time.Duration(rand.Int64N(int64(r.Profile.MaxDelay)+1)))) {
		r.record(rev)
		return
	}
	bet := r.newOp(TaskReversalFirst, id, time.Now(), w, prov, "BET", amount)
	bet.ExtID, bet.IdemKey = betExt, prov.ClientID()+":"+betExt
	bet.Warmup, bet.Step = rev.Warmup, rev.Step
	r.sendHTTP(ctx, &bet, -1)
	r.record(bet)
	betDone := time.Now()

	deadline := betDone.Add(r.Profile.ResolveWithin)
	path := "/providers/" + url.PathEscape(prov.ClientID()) + "/wagering/transactions/" + url.PathEscape(rev.ExtID)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		res := r.HTTP.Do(ctx, target.Request{Method: http.MethodGet, Path: path, Token: prov, Pin: -1})
		if res.Status == 200 {
			var v target.TransactionView
			if json.Unmarshal(res.Body, &v) == nil && v.Status != "" && v.Status != "PENDING_REFERENCE" {
				rev.Resolved = strings.TrimSpace(v.Status + " " + v.FailureCode)
				d := time.Since(betDone)
				rev.ResolvedInMs = float64(d.Microseconds()) / 1000
				if !rev.Warmup {
					r.mu.Lock()
					r.resolution.Record(d)
					r.mu.Unlock()
				}
				break
			}
		}
		if !sleepUntil(ctx, time.Now().Add(200*time.Millisecond)) {
			break
		}
	}
	if rev.Resolved == "" {
		rev.Resolved = "UNRESOLVED"
	}
	r.record(rev)
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
