// Command loadgen is the load and stress generator of the wallet service.
//
//	loadgen seed   -scenario steady -out DIR   open fresh wallets through the API
//	loadgen run    -scenario steady -out DIR   generate load, write the journal
//	loadgen verify -out DIR                    drain, check invariants and the journal
//	loadgen report -out DIR                    write summary.json and report.md
//	loadgen baseline -out DIR                  measure the SQS/SNS emulator ceiling alone
//	loadgen wait-ready                         wait for instances, tokens and queues
//	loadgen purge                              empty the SQS queues (reset helper)
//	loadgen scenarios                          list scenarios and their defaults
//
// loadtest/run.sh chains the steps; loadtest/README.md explains the method.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/auth"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/baseline"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/report"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/scenario"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/target"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/verify"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// exitError carries a specific exit code (1 = verification failed).
type exitError struct {
	code int
	msg  string
}

func (e exitError) Error() string { return e.msg }

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("loadgen ")
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	args := os.Args[2:]
	switch os.Args[1] {
	case "seed":
		err = cmdSeed(ctx, args)
	case "run":
		err = cmdRun(ctx, args)
	case "verify":
		err = cmdVerify(ctx, args)
	case "report":
		err = cmdReport(args)
	case "purge":
		err = cmdPurge(ctx, args)
	case "wait-ready":
		err = cmdWaitReady(ctx, args)
	case "baseline":
		err = cmdBaseline(ctx, args)
	case "scenarios":
		cmdScenarios()
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	var ee exitError
	switch {
	case errors.As(err, &ee):
		log.Print(ee.msg)
		os.Exit(ee.code)
	case err != nil:
		log.Printf("error: %v", err)
		os.Exit(3)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: loadgen <seed|run|verify|report|baseline|wait-ready|purge|scenarios> [flags]
Run "loadgen <command> -h" for the flags of a command.
Scenarios: %s
`, strings.Join(scenario.Names(), ", "))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// common holds the connection settings shared by every command; defaults
// come from the environment so the same binary works on the host and inside
// the Compose network.
type common struct {
	out, targets, tokenURL, dbURL   string
	sqsEndpoint, region, queue, dlq string
	providers, internal             string
	useSQS                          bool
}

func addCommon(fs *flag.FlagSet) *common {
	c := &common{}
	pgPort := env("POSTGRES_HOST_PORT", "55432")
	fs.StringVar(&c.out, "out", env("LOADGEN_OUT", ""), "run directory (results are written here)")
	fs.StringVar(&c.targets, "targets", env("LOADGEN_TARGETS", "http://localhost:8091,http://localhost:8092,http://localhost:8093"), "comma separated wallet instances")
	fs.StringVar(&c.tokenURL, "token-url", env("LOADGEN_TOKEN_URL", "http://localhost:8081/realms/wallet/protocol/openid-connect/token"), "OAuth2 token endpoint")
	fs.StringVar(&c.dbURL, "db-url", env("LOADGEN_DATABASE_URL", "postgres://wallet_owner:wallet_owner@localhost:"+pgPort+"/wallet?sslmode=disable"), "Postgres URL (schema owner, read-only use)")
	fs.StringVar(&c.sqsEndpoint, "sqs-endpoint", env("AWS_ENDPOINT_URL", "http://localhost:4566"), "SQS endpoint (empty for real AWS)")
	fs.StringVar(&c.region, "region", env("AWS_REGION", "us-east-1"), "AWS region")
	fs.StringVar(&c.queue, "queue", env("WAGER_QUEUE_NAME", "wager-transactions.fifo"), "inbound FIFO queue")
	fs.StringVar(&c.dlq, "dlq", env("WAGER_DLQ_NAME", "wager-transactions-dlq.fifo"), "dead letter queue")
	fs.StringVar(&c.providers, "providers", env("LOADGEN_PROVIDERS", "provider-a:provider-a-secret,provider-b:provider-b-secret"), "provider clients, id:secret,...")
	fs.StringVar(&c.internal, "internal-client", env("LOADGEN_INTERNAL_CLIENT", "wallet-internal:wallet-internal-secret"), "internal client id:secret")
	fs.BoolVar(&c.useSQS, "sqs", env("LOADGEN_SQS", "true") == "true", "use the SQS channel")
	return c
}

func (c *common) needOut() error {
	if c.out == "" {
		return errors.New("-out is required")
	}
	return os.MkdirAll(c.out, 0o755)
}

func (c *common) http(retry target.RetryPolicy) *target.HTTP {
	var ts []string
	for _, t := range strings.Split(c.targets, ",") {
		if t = strings.TrimRight(strings.TrimSpace(t), "/"); t != "" {
			ts = append(ts, t)
		}
	}
	return target.NewHTTP(ts, retry)
}

func parseClient(tokenURL, s string) (*auth.Source, error) {
	id, secret, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok || id == "" {
		return nil, fmt.Errorf("client %q is not id:secret", s)
	}
	return auth.NewSource(tokenURL, id, secret, nil), nil
}

func (c *common) providerSources() ([]*auth.Source, error) {
	var out []*auth.Source
	for _, p := range strings.Split(c.providers, ",") {
		s, err := parseClient(c.tokenURL, p)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (c *common) sqsClient(ctx context.Context) (*sqs.Client, string, string, error) {
	cl, err := target.NewSQSClient(ctx, c.region, c.sqsEndpoint)
	if err != nil {
		return nil, "", "", err
	}
	q, err := target.QueueURL(ctx, cl, c.queue)
	if err != nil {
		return nil, "", "", err
	}
	d, err := target.QueueURL(ctx, cl, c.dlq)
	if err != nil {
		return nil, "", "", err
	}
	return cl, q, d, nil
}

// setFlags returns the names of the flags given on the command line, so
// scenario defaults apply only to the others.
func setFlags(fs *flag.FlagSet) map[string]bool {
	m := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { m[f.Name] = true })
	return m
}

// ---- seed -------------------------------------------------------------------

func cmdSeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	c := addCommon(fs)
	name := fs.String("scenario", "smoke", "scenario whose wallet counts are used")
	wallets := fs.Int("wallets", 0, "regular wallets (default from scenario)")
	opening := fs.Int64("opening", 0, "opening balance in cents (default from scenario)")
	hot := fs.Int("hot", 0, "hot wallets (default from scenario)")
	hotOpening := fs.Int64("hot-opening", 0, "hot wallet opening balance in cents (default from scenario)")
	conc := fs.Int("concurrency", 32, "parallel POST /wallets")
	_ = fs.Parse(args)
	if err := c.needOut(); err != nil {
		return err
	}
	p, err := scenario.Get(*name)
	if err != nil {
		return err
	}
	set := setFlags(fs)
	spec := scenario.SeedSpec{Wallets: p.Wallets, OpeningMinor: p.OpeningMinor, Hot: p.HotWallets, HotOpenMinor: p.HotOpenMinor, Currency: "BRL", Concurrency: *conc}
	if set["wallets"] {
		spec.Wallets = *wallets
	}
	if set["opening"] {
		spec.OpeningMinor = *opening
	}
	if set["hot"] {
		spec.Hot = *hot
	}
	if set["hot-opening"] {
		spec.HotOpenMinor = *hotOpening
	}
	internal, err := parseClient(c.tokenURL, c.internal)
	if err != nil {
		return err
	}
	retry := target.DefaultRetry()
	retry.AttemptTimeout = 10 * time.Second
	log.Printf("seed: opening %d wallets + %d hot via POST /wallets (concurrency %d)", spec.Wallets, spec.Hot, spec.Concurrency)
	ws, st, err := scenario.Seed(ctx, c.http(retry), internal, spec, log.Printf)
	st.Scenario = p.Name
	if werr := journal.WriteWallets(filepath.Join(c.out, result.WalletsFile), ws); werr != nil {
		return werr
	}
	if werr := result.Write(c.out, result.SeedFile, st); werr != nil {
		return werr
	}
	if err != nil {
		return err
	}
	log.Printf("seed: %d wallets in %.1fs (%.0f/s), p99 %.1f ms", st.Wallets, st.Seconds, st.PerSecond, st.Latency.Summary().P99Ms)
	return nil
}

// ---- run --------------------------------------------------------------------

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	c := addCommon(fs)
	name := fs.String("scenario", "smoke", "scenario: "+strings.Join(scenario.Names(), ", "))
	rate := fs.Float64("rate", 0, "target operations per second (ramp: start rate)")
	duration := fs.Duration("duration", 0, "load duration")
	warmup := fs.Duration("warmup", 0, "warmup excluded from statistics")
	mix := fs.String("mix", "", "task mix, e.g. round-http=80,round-sqs=20")
	maxInFlight := fs.Int("max-in-flight", 0, "max concurrent tasks before arrivals are dropped")
	rampStep := fs.Float64("ramp-step", 0, "ramp: ops/s added per step")
	rampEvery := fs.Duration("ramp-every", 0, "ramp: step length")
	rampMax := fs.Float64("ramp-max", 0, "ramp: max ops/s")
	orphans := fs.Float64("orphan-ratio", 0, "reversal-first: fraction of reversals whose BET is never sent")
	maxDelay := fs.Duration("max-delay", 0, "reversal-first: max delay before the BET")
	attemptTimeout := fs.Duration("attempt-timeout", 5*time.Second, "timeout of one HTTP attempt")
	maxAttempts := fs.Int("max-attempts", 6, "HTTP attempts per operation (transient failures)")
	metricsAddr := fs.String("metrics-addr", env("LOADGEN_METRICS_ADDR", ":9100"), "Prometheus endpoint of the generator (empty to disable)")
	runID := fs.String("run-id", "", "prefix of external transaction ids (default: generated)")
	idleWait := fs.Duration("idle-timeout", 3*time.Minute, "wait up to this long for the outbox and the inbound queue to be empty before starting (0 to skip)")
	drainAudit := fs.Bool("drain-audit", true, "consume "+auditQueue+" during the run, like a downstream consumer")
	_ = fs.Parse(args)
	if err := c.needOut(); err != nil {
		return err
	}
	p, err := scenario.Get(*name)
	if err != nil {
		return err
	}
	set := setFlags(fs)
	if set["rate"] {
		p.Rate = *rate
	}
	if set["duration"] {
		p.Duration = *duration
	}
	if set["warmup"] {
		p.Warmup = *warmup
	}
	if set["mix"] {
		if p.Mix, err = scenario.ParseMix(*mix); err != nil {
			return err
		}
	}
	if set["max-in-flight"] {
		p.MaxInFlight = *maxInFlight
	}
	if set["ramp-step"] {
		p.RampStep = *rampStep
	}
	if set["ramp-every"] {
		p.RampEvery = *rampEvery
	}
	if set["ramp-max"] {
		p.RampMax = *rampMax
	}
	if set["orphan-ratio"] {
		p.OrphanRatio = *orphans
	}
	if set["max-delay"] {
		p.MaxDelay = *maxDelay
	}
	if !c.useSQS {
		p.Mix = scenario.WithoutSQS(p.Mix)
	}

	all, err := journal.ReadWallets(filepath.Join(c.out, result.WalletsFile))
	if err != nil {
		return fmt.Errorf("read wallets (run seed first): %w", err)
	}
	r := &scenario.Runner{Profile: p, Currency: "BRL", Logf: log.Printf}
	for _, w := range all {
		if w.Hot {
			r.Hot = append(r.Hot, w)
		} else {
			r.Wallets = append(r.Wallets, w)
		}
	}
	if p.Mix[scenario.TaskHot] > 0 && len(r.Hot) == 0 {
		return errors.New("scenario needs hot wallets: seed with -scenario hot-wallet or -hot N")
	}
	if len(p.Mix) > 0 && p.Mix[scenario.TaskHot] < sum(p.Mix) && len(r.Wallets) == 0 {
		return errors.New("scenario needs regular wallets: seed with -wallets N")
	}
	r.RunID = *runID
	if r.RunID == "" {
		r.RunID = fmt.Sprintf("lg-%s-%s", p.Name, time.Now().UTC().Format("20060102T150405"))
	}
	retry := target.DefaultRetry()
	retry.AttemptTimeout, retry.MaxAttempts = *attemptTimeout, *maxAttempts
	r.HTTP = c.http(retry)
	if r.Providers, err = c.providerSources(); err != nil {
		return err
	}
	// Fetch tokens up front: Keycloak stays off the measured path.
	for _, s := range r.Providers {
		if _, err := s.Token(ctx); err != nil {
			return err
		}
	}

	var dlqBefore int64
	var sqsClient *sqs.Client
	var queueURL string
	if c.useSQS {
		var dlqURL string
		if sqsClient, queueURL, dlqURL, err = c.sqsClient(ctx); err != nil {
			return err
		}
		dlqBefore, _ = target.QueueDepth(ctx, sqsClient, dlqURL)
		r.Producer = target.NewProducer(sqsClient, queueURL, 8, 5*time.Millisecond)
		defer r.Producer.Close()
	}
	var drainer *target.Drainer
	if c.useSQS && *drainAudit {
		auditURL, err := target.QueueURL(ctx, sqsClient, auditQueue)
		if err != nil {
			return err
		}
		drainer = target.StartDrainer(ctx, sqsClient, auditURL, 2)
	}
	if c.dbURL != "" {
		pool, err := pgxpool.New(ctx, c.dbURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		r.Sampler = func(ctx context.Context) (result.DBSample, error) {
			var s result.DBSample
			err := pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
				(SELECT coalesce(extract(epoch FROM now() - min(occurred_at)), 0)::float8 FROM outbox_events WHERE published_at IS NULL),
				(SELECT count(*) FROM wager_transactions WHERE status = 'PENDING_REFERENCE'),
				(SELECT count(*) FROM pg_locks WHERE NOT granted),
				(SELECT count(*) FROM pg_stat_activity WHERE datname = current_database())`).
				Scan(&s.OutboxBacklog, &s.OutboxOldestSec, &s.PendingRefs, &s.LockWaits, &s.Connections)
			if sqsClient != nil {
				s.QueueDepth, _ = target.QueueDepth(ctx, sqsClient, queueURL)
			}
			return s, err
		}
	}

	// Start from an idle system: seeding leaves two outbox events per wallet,
	// and a backlog at t=0 would be charged to the scenario.
	if r.Sampler != nil && *idleWait > 0 {
		if err := waitIdle(ctx, r.Sampler, *idleWait); err != nil {
			return err
		}
	}

	r.Metrics = scenario.NewMetrics()
	if *metricsAddr != "" {
		srv := &http.Server{Addr: *metricsAddr, Handler: promhttp.HandlerFor(r.Metrics.Registry, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("metrics endpoint: %v", err)
			}
		}()
		defer func() {
			time.Sleep(3 * time.Second) // let Prometheus scrape the final values
			_ = srv.Close()
		}()
	}

	jw, err := journal.Create(filepath.Join(c.out, result.JournalFile))
	if err != nil {
		return err
	}
	r.Journal = jw
	out := r.Run(ctx)
	if err := jw.Close(); err != nil {
		return err
	}
	if drainer != nil {
		out.AuditDrained, out.EventDelivery = drainer.Stop()
	}
	out.Command = append([]string{"loadgen"}, os.Args[1:]...)
	out.DLQBefore = dlqBefore
	out.Params = map[string]any{
		"scenario": p.Name, "rate_ops_per_sec": p.Rate, "duration": p.Duration.String(), "warmup": p.Warmup.String(),
		"mix": p.Mix, "max_in_flight": p.MaxInFlight, "wallets": len(r.Wallets), "hot_wallets": len(r.Hot),
		"http_attempt_timeout": retry.AttemptTimeout.String(), "http_max_attempts": retry.MaxAttempts, "sqs": c.useSQS,
		"drain_audit_queue": c.useSQS && *drainAudit,
	}
	if p.RampEvery > 0 {
		out.StepSec = p.RampEvery.Seconds()
		out.Params["ramp"] = fmt.Sprintf("+%.0f op/s every %s up to %.0f", p.RampStep, p.RampEvery, p.RampMax)
	}
	if p.Mix[scenario.TaskReversalFirst] > 0 {
		out.Params["reversal_max_delay"] = p.MaxDelay.String()
		out.Params["orphan_ratio"] = p.OrphanRatio
	}
	if p.Mix[scenario.TaskDuplicate] > 0 {
		out.Params["duplicate_copies"] = fmt.Sprintf("%d HTTP + %d SQS same messageId + %d SQS distinct messageIds, within %s",
			scenario.DupHTTPCopies, scenario.DupSQSSameID, scenario.DupSQSDistinct, p.DupSpread)
	}
	if err := result.Write(c.out, result.RunFile, out); err != nil {
		return err
	}
	log.Printf("run: %d ops journaled, %d measured in %.1fs (%.1f op/s), dropped %d, valid %v",
		out.OpsJournaled, out.OpsMeasured, out.MeasuredSec, float64(out.OpsMeasured)/max(out.MeasuredSec, 1e-9), out.TasksDropped, out.Valid)
	return ctx.Err()
}

func waitIdle(ctx context.Context, sample func(context.Context) (result.DBSample, error), timeout time.Duration) error {
	start := time.Now()
	for {
		s, err := sample(ctx)
		if err == nil && s.OutboxBacklog == 0 && s.QueueDepth == 0 {
			if time.Since(start) > time.Second {
				log.Printf("idle after %.1fs", time.Since(start).Seconds())
			}
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("system not idle after %s (outbox backlog %d, queue depth %d): the previous load has not drained", timeout, s.OutboxBacklog, s.QueueDepth)
		}
		log.Printf("waiting for an idle system: outbox backlog %d, queue depth %d", s.OutboxBacklog, s.QueueDepth)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func sum(m map[string]float64) float64 {
	var s float64
	for _, v := range m {
		s += v
	}
	return s
}

// ---- verify -----------------------------------------------------------------

func cmdVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	c := addCommon(fs)
	drain := fs.Duration("drain-timeout", 120*time.Second, "max wait for outbox, inbox and queue to drain")
	reconcile := fs.Int("reconcile", 50, "random wallets checked through POST /wallets/{id}/reconciliation (plus every hot wallet)")
	lookups := fs.Int("lookups", 50, "random transactions read back through the provider lookup endpoint")
	_ = fs.Parse(args)
	if err := c.needOut(); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, c.dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	cfg := verify.Config{Dir: c.out, DB: pool, HTTP: c.http(target.DefaultRetry()), DrainTimeout: *drain,
		ReconcileSample: *reconcile, LookupSample: *lookups, Logf: log.Printf}
	if cfg.Internal, err = parseClient(c.tokenURL, c.internal); err != nil {
		return err
	}
	if cfg.Providers, err = c.providerSources(); err != nil {
		return err
	}
	if c.useSQS {
		if cfg.SQS, cfg.QueueURL, cfg.DLQURL, err = c.sqsClient(ctx); err != nil {
			return err
		}
		if cfg.AuditURL, err = target.QueueURL(ctx, cfg.SQS, auditQueue); err != nil {
			return err
		}
	}
	v, err := verify.Run(ctx, cfg)
	if err != nil {
		return err
	}
	if err := result.Write(c.out, result.VerifyFile, v); err != nil {
		return err
	}
	if !v.Pass {
		for _, r := range v.FailReasons {
			log.Printf("FAIL: %s", r)
		}
		return exitError{1, "verify: FAIL"}
	}
	log.Printf("verify: PASS (%d invariants, %d wallets cross-checked, outbox lag p99 %.2fs)", len(v.Checks), v.Cross.Stats.WalletsChecked, v.Outbox.P99Sec)
	return nil
}

// ---- report -----------------------------------------------------------------

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	out := fs.String("out", env("LOADGEN_OUT", ""), "run directory")
	_ = fs.Parse(args)
	if *out == "" {
		return errors.New("-out is required")
	}
	s, err := report.Write(*out)
	if err != nil {
		return err
	}
	log.Printf("report: %s -> %s", s.Verdict, filepath.Join(*out, result.ReportFile))
	if s.Verdict != "PASS" {
		return exitError{1, "report: " + s.Verdict}
	}
	return nil
}

// auditQueue receives every outbound event through the SNS topic; nobody
// consumes it, so it is purged at the start of each run.
const auditQueue = "wallet-events-audit.fifo"

// ---- purge ------------------------------------------------------------------

func cmdPurge(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	c := addCommon(fs)
	queues := fs.String("queues", "", "comma separated queues to purge (default: inbound, DLQ and audit)")
	_ = fs.Parse(args)
	cl, err := target.NewSQSClient(ctx, c.region, c.sqsEndpoint)
	if err != nil {
		return err
	}
	names := []string{c.queue, c.dlq, auditQueue}
	if *queues != "" {
		names = strings.Split(*queues, ",")
	}
	for _, name := range names {
		u, err := target.QueueURL(ctx, cl, name)
		if err != nil {
			return err
		}
		if err := target.Purge(ctx, cl, u); err != nil {
			return fmt.Errorf("purge %s: %w", name, err)
		}
		log.Printf("purged %s", name)
	}
	return nil
}

func cmdScenarios() {
	for _, n := range scenario.Names() {
		p, _ := scenario.Get(n)
		fmt.Printf("%-15s %s\n    wallets=%d hot=%d rate=%.0f op/s duration=%s warmup=%s mix=%v\n",
			n, p.Description, p.Wallets, p.HotWallets, p.Rate, p.Duration, p.Warmup, p.Mix)
	}
}

// ---- wait-ready -------------------------------------------------------------

func cmdWaitReady(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("wait-ready", flag.ExitOnError)
	c := addCommon(fs)
	timeout := fs.Duration("timeout", 3*time.Minute, "max wait")
	_ = fs.Parse(args)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	h := c.http(target.RetryPolicy{MaxAttempts: 1, AttemptTimeout: 3 * time.Second})
	for i, t := range h.Targets {
		for {
			res := h.Do(ctx, target.Request{Method: http.MethodGet, Path: "/health/ready", Pin: i})
			if res.Status == http.StatusOK {
				log.Printf("ready: %s", t)
				break
			}
			if ctx.Err() != nil {
				return fmt.Errorf("%s not ready (last: HTTP %d %v %.200s)", t, res.Status, res.Err, res.Body)
			}
			time.Sleep(2 * time.Second)
		}
	}
	sources, err := c.providerSources()
	if err != nil {
		return err
	}
	internal, err := parseClient(c.tokenURL, c.internal)
	if err != nil {
		return err
	}
	for _, s := range append(sources, internal) {
		if _, err := s.Token(ctx); err != nil {
			return err
		}
	}
	log.Printf("ready: tokens for %d clients", len(sources)+1)
	if c.useSQS {
		if _, _, _, err := c.sqsClient(ctx); err != nil {
			return err
		}
		log.Printf("ready: queues %s and %s", c.queue, c.dlq)
	}
	return nil
}

// ---- baseline ---------------------------------------------------------------

func cmdBaseline(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("baseline", flag.ExitOnError)
	c := addCommon(fs)
	duration := fs.Duration("duration", 10*time.Second, "length of each send/publish phase")
	workers := fs.Int("workers", 8, "concurrent callers")
	groups := fs.Int("groups", 2000, "distinct MessageGroupIds (like walletIds)")
	topic := fs.String("topic", env("EVENTS_TOPIC_NAME", "wallet-events.fifo"), "SNS FIFO topic")
	_ = fs.Parse(args)
	if err := c.needOut(); err != nil {
		return err
	}
	sq, err := target.NewSQSClient(ctx, c.region, c.sqsEndpoint)
	if err != nil {
		return err
	}
	sn, err := baseline.NewSNSClient(ctx, c.region, c.sqsEndpoint)
	if err != nil {
		return err
	}
	res, err := baseline.Run(ctx, baseline.Config{SQS: sq, SNS: sn, TopicName: *topic, AuditQueue: auditQueue,
		Duration: *duration, Workers: *workers, Groups: *groups, Logf: log.Printf})
	if err != nil {
		return err
	}
	if err := result.Write(c.out, baseline.File, res); err != nil {
		return err
	}
	log.Printf("baseline: fan-out %d of %d published events reached the audit queue", res.FanOutDelivered, res.FanOutPublished)
	return nil
}
