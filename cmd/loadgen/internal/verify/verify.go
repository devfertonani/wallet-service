// Package verify proves, after a run, that the financial invariants held:
// SQL invariants over the whole database, the journal oracle against the
// run's wallets, and a sample of the reconciliation and lookup endpoints.
package verify

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/auth"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/target"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed invariants.sql
var invariantsSQL string

// Query is one named invariant from invariants.sql.
type Query struct {
	Name, Description, SQL string
}

// Queries parses invariants.sql into its named queries.
func Queries() []Query {
	var out []Query
	var cur *Query
	for _, line := range strings.Split(invariantsSQL, "\n") {
		switch {
		case strings.HasPrefix(line, "-- name: "):
			out = append(out, Query{Name: strings.TrimSpace(strings.TrimPrefix(line, "-- name: "))})
			cur = &out[len(out)-1]
		case cur == nil:
		case strings.HasPrefix(line, "-- description: "):
			cur.Description = strings.TrimSpace(strings.TrimPrefix(line, "-- description: "))
		default:
			cur.SQL += line + "\n"
		}
	}
	for i := range out {
		out[i].SQL = strings.TrimSuffix(strings.TrimSpace(out[i].SQL), ";")
	}
	return out
}

type Config struct {
	Dir             string
	DB              *pgxpool.Pool
	HTTP            *target.HTTP
	Internal        *auth.Source
	Providers       []*auth.Source
	SQS             *sqs.Client // nil when SQS is not used
	QueueURL        string
	DLQURL          string
	AuditURL        string // drained during the wait, see target.Drainer
	DrainTimeout    time.Duration
	ReconcileSample int
	LookupSample    int
	Logf            func(string, ...any)
}

func (c Config) logf(f string, a ...any) {
	if c.Logf != nil {
		c.Logf(f, a...)
	}
}

func Run(ctx context.Context, cfg Config) (result.Verify, error) {
	var v result.Verify
	wallets, err := journal.ReadWallets(cfg.Dir + "/" + result.WalletsFile)
	if err != nil {
		return v, fmt.Errorf("read wallets: %w", err)
	}
	ops, err := journal.ReadOps(cfg.Dir + "/" + result.JournalFile)
	if err != nil {
		return v, fmt.Errorf("read journal: %w", err)
	}
	var run result.Run
	if err := result.Read(cfg.Dir, result.RunFile, &run); err != nil {
		return v, fmt.Errorf("read run: %w", err)
	}

	var drainer *target.Drainer
	if cfg.SQS != nil && cfg.AuditURL != "" {
		drainer = target.StartDrainer(ctx, cfg.SQS, cfg.AuditURL, 2)
	}
	v.Outbox = drain(ctx, cfg, run.LoadEndedAt)
	if drainer != nil {
		v.AuditDrained, v.EventDelivery = drainer.Stop()
	}
	if !v.Outbox.Drained {
		v.FailReasons = append(v.FailReasons, "backlog did not drain within "+cfg.DrainTimeout.String()+": "+v.Outbox.DrainDetails)
	}

	if v.Checks, err = invariants(ctx, cfg.DB); err != nil {
		return v, err
	}
	for _, c := range v.Checks {
		if c.Violations > 0 || c.Error != "" {
			v.FailReasons = append(v.FailReasons, fmt.Sprintf("invariant %s: %d violations %s", c.Name, c.Violations, c.Error))
		}
	}
	if err := outboxLag(ctx, cfg.DB, run.StartedAt, run.LoadEndedAt, &v.Outbox); err != nil {
		return v, err
	}

	txs, dbw, err := loadRun(ctx, cfg.DB, wallets)
	if err != nil {
		return v, err
	}
	v.Cross = journal.CrossCheck(wallets, ops, txs, dbw)
	v.SQSEndToEnd = v.Cross.SQSEndToEnd
	if n := len(v.Cross.Violations); n > 0 {
		v.FailReasons = append(v.FailReasons, fmt.Sprintf("journal cross-check: %d violations (first: %s %s)", n, v.Cross.Violations[0].Check, v.Cross.Violations[0].Detail))
	}

	v.Reconciliation = reconcile(ctx, cfg, wallets)
	if v.Reconciliation.Failed > 0 {
		v.FailReasons = append(v.FailReasons, fmt.Sprintf("reconciliation: %d of %d wallets inconsistent or failed", v.Reconciliation.Failed, v.Reconciliation.Sampled))
	}
	v.Lookups = lookups(ctx, cfg, txs)
	if v.Lookups.Failed > 0 {
		v.FailReasons = append(v.FailReasons, fmt.Sprintf("lookup: %d of %d transactions differ from the database", v.Lookups.Failed, v.Lookups.Sampled))
	}

	if cfg.SQS != nil && cfg.DLQURL != "" {
		if n, err := target.QueueDepth(ctx, cfg.SQS, cfg.DLQURL); err == nil {
			v.DLQAfter, v.DLQDelta = n, n-run.DLQBefore
			if v.DLQDelta != 0 {
				v.FailReasons = append(v.FailReasons, fmt.Sprintf("DLQ grew by %d messages during the run", v.DLQDelta))
			}
		}
	}
	v.Pass = len(v.FailReasons) == 0
	v.VerifiedAt = time.Now()
	return v, nil
}

// drain waits until the outbox backlog, the unfinished inbox and the inbound
// queue are all empty: only then are the invariants meaningful.
func drain(ctx context.Context, cfg Config, loadEnded time.Time) result.Outbox {
	var o result.Outbox
	deadline := time.Now().Add(cfg.DrainTimeout)
	first := true
	var t0 time.Time
	for {
		var backlog, inbox int64
		err := cfg.DB.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
			(SELECT count(*) FROM inbox_messages WHERE completed_at IS NULL)`).Scan(&backlog, &inbox)
		var depth int64
		if cfg.SQS != nil && cfg.QueueURL != "" {
			depth, _ = target.QueueDepth(ctx, cfg.SQS, cfg.QueueURL)
		}
		o.DrainDetails = fmt.Sprintf("outbox backlog %d, inbox unfinished %d, queue depth %d", backlog, inbox, depth)
		if err != nil {
			o.DrainDetails = err.Error()
		} else if first {
			first, t0 = false, time.Now()
			o.BacklogAtStop, o.QueueAtStop = backlog, depth
		} else if secs := time.Since(t0).Seconds(); secs > 0 {
			o.DrainPerSec = float64(o.BacklogAtStop-backlog) / secs
			o.QueueDrainPerSec = float64(o.QueueAtStop-depth) / secs
		}
		if err == nil && backlog == 0 && inbox == 0 && depth == 0 {
			o.Drained = true
			if !loadEnded.IsZero() {
				o.DrainSec = time.Since(loadEnded).Seconds()
			}
			o.DrainDetails = ""
			return o
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return o
		}
		cfg.logf("waiting for drain: %s", o.DrainDetails)
		time.Sleep(time.Second)
	}
}

func invariants(ctx context.Context, db *pgxpool.Pool) ([]result.Check, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // read-only
	var out []result.Check
	for _, q := range Queries() {
		c := result.Check{Name: q.Name, Description: q.Description}
		rows, err := tx.Query(ctx, q.SQL)
		if err != nil {
			return nil, fmt.Errorf("invariant %s: %w", q.Name, err)
		}
		for rows.Next() {
			c.Violations++
			if len(c.Samples) < 5 {
				vals, _ := rows.Values()
				c.Samples = append(c.Samples, formatRow(vals))
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("invariant %s: %w", q.Name, err)
		}
		out = append(out, c)
	}
	return out, nil
}

func formatRow(vals []any) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		switch x := v.(type) {
		case [16]byte:
			parts[i] = uuid.UUID(x).String()
		case time.Time:
			parts[i] = x.UTC().Format(time.RFC3339Nano)
		case nil:
			parts[i] = "NULL"
		default:
			parts[i] = fmt.Sprint(x)
		}
	}
	return strings.Join(parts, " | ")
}

func outboxLag(ctx context.Context, db *pgxpool.Pool, since, loadEnded time.Time, o *result.Outbox) error {
	var p50, p95, p99, mx *float64
	var produced, published int64
	err := db.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE published_at IS NULL),
		       percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - occurred_at)),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - occurred_at)),
		       percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - occurred_at)),
		       max(extract(epoch FROM published_at - occurred_at))::float8,
		       count(*) FILTER (WHERE occurred_at <= $2),
		       count(*) FILTER (WHERE published_at <= $2)
		FROM outbox_events WHERE occurred_at >= $1`, since, loadEnded).Scan(&o.Events, &o.Unpublished, &p50, &p95, &p99, &mx, &produced, &published)
	if err != nil {
		return fmt.Errorf("outbox lag: %w", err)
	}
	deref := func(p *float64) float64 {
		if p == nil {
			return 0
		}
		return *p
	}
	o.P50Sec, o.P95Sec, o.P99Sec, o.MaxSec = deref(p50), deref(p95), deref(p99), deref(mx)
	if secs := loadEnded.Sub(since).Seconds(); secs > 0 {
		o.ProducedPerSec, o.PublishedPerSec = float64(produced)/secs, float64(published)/secs
	}
	return nil
}

func loadRun(ctx context.Context, db *pgxpool.Pool, wallets []journal.Wallet) ([]journal.DBTx, []journal.DBWallet, error) {
	ids := make([]string, len(wallets))
	for i, w := range wallets {
		ids[i] = w.ID
	}
	rows, err := db.Query(ctx, `
		SELECT id::text, provider_id, external_transaction_id, kind, status, coalesce(failure_code, ''),
		       amount_minor, wallet_id::text, created_at
		FROM wager_transactions WHERE wallet_id = ANY($1::uuid[]) AND origin = 'EXTERNAL'`, ids)
	if err != nil {
		return nil, nil, err
	}
	txs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (journal.DBTx, error) {
		var t journal.DBTx
		err := r.Scan(&t.ID, &t.Provider, &t.ExtID, &t.Kind, &t.Status, &t.FailureCode, &t.Amount, &t.WalletID, &t.CreatedAt)
		return t, err
	})
	if err != nil {
		return nil, nil, err
	}
	rows, err = db.Query(ctx, `SELECT id::text, balance_minor, version FROM wallets WHERE id = ANY($1::uuid[])`, ids)
	if err != nil {
		return nil, nil, err
	}
	ws, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (journal.DBWallet, error) {
		var w journal.DBWallet
		err := r.Scan(&w.ID, &w.Balance, &w.Version)
		return w, err
	})
	return txs, ws, err
}

// reconcile calls POST /wallets/{id}/reconciliation on every hot wallet and
// a random sample of the others.
func reconcile(ctx context.Context, cfg Config, wallets []journal.Wallet) result.APISample {
	var s result.APISample
	if cfg.HTTP == nil || cfg.Internal == nil || cfg.ReconcileSample <= 0 {
		return s
	}
	var pick []journal.Wallet
	var rest []journal.Wallet
	for _, w := range wallets {
		if w.Hot {
			pick = append(pick, w)
		} else {
			rest = append(rest, w)
		}
	}
	rand.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
	pick = append(pick, rest[:min(cfg.ReconcileSample, len(rest))]...)
	for _, w := range pick {
		s.Sampled++
		res := cfg.HTTP.Do(ctx, target.Request{Method: http.MethodPost, Path: "/wallets/" + w.ID + "/reconciliation", Token: cfg.Internal, Pin: -1})
		var rr target.ReconciliationResponse
		if res.Status != 200 || json.Unmarshal(res.Body, &rr) != nil || !rr.Consistent {
			s.Failed++
			if len(s.Failures) < 10 {
				s.Failures = append(s.Failures, fmt.Sprintf("%s: HTTP %d %.200s %v", w.ID, res.Status, res.Body, res.Err))
			}
		}
	}
	return s
}

// lookups reads a sample of transactions through the provider lookup
// endpoint and compares them with the database.
func lookups(ctx context.Context, cfg Config, txs []journal.DBTx) result.APISample {
	var s result.APISample
	if cfg.HTTP == nil || cfg.LookupSample <= 0 || len(txs) == 0 {
		return s
	}
	prov := map[string]*auth.Source{}
	for _, p := range cfg.Providers {
		prov[p.ClientID()] = p
	}
	sample := append([]journal.DBTx(nil), txs...)
	rand.Shuffle(len(sample), func(i, j int) { sample[i], sample[j] = sample[j], sample[i] })
	for _, t := range sample[:min(cfg.LookupSample, len(sample))] {
		p := prov[t.Provider]
		if p == nil {
			continue
		}
		s.Sampled++
		path := "/providers/" + url.PathEscape(t.Provider) + "/wagering/transactions/" + url.PathEscape(t.ExtID)
		res := cfg.HTTP.Do(ctx, target.Request{Method: http.MethodGet, Path: path, Token: p, Pin: -1})
		var view target.TransactionView
		if res.Status != 200 || json.Unmarshal(res.Body, &view) != nil || view.Status != t.Status || view.TxID() != t.ID {
			s.Failed++
			if len(s.Failures) < 10 {
				s.Failures = append(s.Failures, fmt.Sprintf("%s/%s: HTTP %d %.200s (db: %s %s)", t.Provider, t.ExtID, res.Status, res.Body, t.Status, t.ID))
			}
		}
	}
	return s
}
