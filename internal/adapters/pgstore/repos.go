package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/domain/events"
	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return wagering.ErrNotFound
	}
	return err
}

// ---- wallets ----

type walletRepo struct{ tx pgx.Tx }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (wallet.Wallet, error) {
	var (
		id, player uuid.UUID
		currency   string
		balance    int64
		version    int64
		created    time.Time
		updated    time.Time
	)
	if err := row.Scan(&id, &player, &currency, &balance, &version, &created, &updated); err != nil {
		return wallet.Wallet{}, notFound(err)
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return wallet.Wallet{}, err
	}
	m, err := money.FromMinor(balance, c)
	if err != nil {
		return wallet.Wallet{}, err
	}
	return wallet.Rehydrate(id, player, m, version, created, updated)
}

func (r walletRepo) Insert(ctx context.Context, w wallet.Wallet) error {
	_, err := r.tx.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		w.ID(), w.PlayerID(), w.Currency().String(), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return err
}

func (r walletRepo) Get(ctx context.Context, id uuid.UUID) (wallet.Wallet, error) {
	return scanWallet(r.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

// GetForUpdate takes the row lock of this wallet only: operations on other
// wallets never wait for it. lock_timeout (set on the pool) bounds the wait.
func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (wallet.Wallet, error) {
	return scanWallet(r.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

// Update is conditioned on the version read under the lock: a second barrier
// against lost updates, after the row lock and before the CHECK constraint.
func (r walletRepo) Update(ctx context.Context, w wallet.Wallet, prevVersion int64) error {
	tag, err := r.tx.Exec(ctx, `UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
		WHERE id = $1 AND version = $5`, w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), prevVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: wallet %s at version %d", wagering.ErrConcurrentUpdate, w.ID(), prevVersion)
	}
	return nil
}

// ---- wager transactions ----

type transactionRepo struct{ tx pgx.Tx }

const transactionColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	correlation_id, attempts, next_attempt_at, expires_at, created_at, updated_at`

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func scanTransaction(row pgx.Row) (*wager.Transaction, error) {
	var (
		s                                          wager.Snapshot
		origin, kind, status, currency             string
		amount                                     int64
		provider, external, key, hash, round, game *string
		refExternal, failureCode, correlation      *string
		refID                                      uuid.NullUUID
		result                                     *int64
	)
	err := row.Scan(&s.ID, &origin, &kind, &status, &s.WalletID, &s.PlayerID, &amount, &currency,
		&provider, &external, &key, &hash, &round, &game,
		&refExternal, &refID, &failureCode, &result,
		&correlation, &s.Attempts, &s.NextAttemptAt, &s.ExpiresAt, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, notFound(err)
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	if s.Money, err = money.FromMinor(amount, c); err != nil {
		return nil, err
	}
	if result != nil {
		rb, err := money.FromMinor(*result, c)
		if err != nil {
			return nil, err
		}
		s.ResultBalance = &rb
	}
	s.Origin, s.Kind, s.Status = wager.Origin(origin), wager.Kind(kind), wager.Status(status)
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey = value(provider), value(external), value(key)
	s.PayloadHash, s.RoundID, s.GameID = value(hash), value(round), value(game)
	s.ReferenceExternalTransactionID, s.CorrelationID = value(refExternal), value(correlation)
	s.FailureCode = failure.Code(value(failureCode))
	if refID.Valid {
		s.ReferenceTransactionID = refID.UUID
	}
	return wager.Rehydrate(s)
}

func resultMinor(s wager.Snapshot) *int64 {
	if s.ResultBalance == nil {
		return nil
	}
	v := s.ResultBalance.Minor()
	return &v
}

func refUUID(id uuid.UUID) uuid.NullUUID { return uuid.NullUUID{UUID: id, Valid: id != uuid.Nil} }

func (r transactionRepo) Insert(ctx context.Context, t *wager.Transaction) error {
	s := t.Snapshot()
	_, err := r.tx.Exec(ctx, `INSERT INTO wager_transactions (`+transactionColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
		s.ID, string(s.Origin), string(s.Kind), string(s.Status), s.WalletID, s.PlayerID, s.Money.Minor(), s.Money.Currency().String(),
		nullable(s.ProviderID), nullable(s.ExternalTransactionID), nullable(s.IdempotencyKey), nullable(s.PayloadHash),
		nullable(s.RoundID), nullable(s.GameID), nullable(s.ReferenceExternalTransactionID), refUUID(s.ReferenceTransactionID),
		nullable(string(s.FailureCode)), resultMinor(s), nullable(s.CorrelationID), s.Attempts, s.NextAttemptAt, s.ExpiresAt,
		s.CreatedAt, s.UpdatedAt)
	return err
}

// Update writes the mutable part of a transaction: state, resolution, result
// and retry bookkeeping. The schema trigger refuses anything else.
func (r transactionRepo) Update(ctx context.Context, t *wager.Transaction) error {
	s := t.Snapshot()
	tag, err := r.tx.Exec(ctx, `UPDATE wager_transactions SET status = $2, reference_transaction_id = $3, failure_code = $4,
		result_balance_minor = $5, attempts = $6, next_attempt_at = $7, expires_at = $8, updated_at = $9
		WHERE id = $1`,
		s.ID, string(s.Status), refUUID(s.ReferenceTransactionID), nullable(string(s.FailureCode)), resultMinor(s),
		s.Attempts, s.NextAttemptAt, s.ExpiresAt, s.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return wagering.ErrNotFound
	}
	return nil
}

func (r transactionRepo) Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r transactionRepo) GetByIdempotencyKey(ctx context.Context, providerID, key string) (*wager.Transaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (r transactionRepo) GetByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (r transactionRepo) IsReversed(ctx context.Context, id uuid.UUID) (bool, error) {
	var reversed bool
	err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`, id).Scan(&reversed)
	return reversed, err
}

func (r transactionRepo) WakeWaiting(ctx context.Context, providerID, externalID string, now time.Time) error {
	_, err := r.tx.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = $3
		WHERE id IN (
			SELECT id FROM wager_transactions
			WHERE status = 'PENDING_REFERENCE' AND provider_id = $1 AND reference_external_transaction_id = $2
			  AND next_attempt_at > $3
			FOR UPDATE SKIP LOCKED)`, providerID, externalID, now)
	return err
}

func (r transactionRepo) DuePending(ctx context.Context, now time.Time, limit int) ([]wagering.PendingRef, error) {
	rows, err := r.tx.Query(ctx, `SELECT id, wallet_id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1
		ORDER BY next_attempt_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (wagering.PendingRef, error) {
		var p wagering.PendingRef
		err := row.Scan(&p.ID, &p.WalletID)
		return p, err
	})
}

func (r transactionRepo) LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	t, err := scanTransaction(r.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE id = $1 AND status = 'PENDING_REFERENCE' AND next_attempt_at <= $2
		FOR UPDATE SKIP LOCKED`, id, now))
	if errors.Is(err, wagering.ErrNotFound) {
		return nil, nil
	}
	return t, err
}

// ---- ledger ----

type ledgerRepo struct{ tx pgx.Tx }

func (r ledgerRepo) Append(ctx context.Context, e ledger.Entry) error {
	_, err := r.tx.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(), e.Amount().Currency().String(),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.WalletVersion(), e.CreatedAt())
	return err
}

func (r ledgerRepo) Page(ctx context.Context, walletID uuid.UUID, after int64, limit int) ([]wagering.LedgerRow, error) {
	rows, err := r.tx.Query(ctx, `SELECT seq, id, transaction_id, direction, amount_minor, currency,
			balance_before, balance_after, wallet_version, created_at
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, walletID, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (wagering.LedgerRow, error) {
		var (
			seq, amount, before, after, version int64
			id, txID                            uuid.UUID
			dir, currency                       string
			created                             time.Time
		)
		if err := row.Scan(&seq, &id, &txID, &dir, &amount, &currency, &before, &after, &version, &created); err != nil {
			return wagering.LedgerRow{}, err
		}
		c, err := money.ParseCurrency(currency)
		if err != nil {
			return wagering.LedgerRow{}, err
		}
		a, _ := money.FromMinor(amount, c)
		b, _ := money.FromMinor(before, c)
		f, _ := money.FromMinor(after, c)
		e, err := ledger.New(id, walletID, txID, ledger.Direction(dir), a, b, f, version, created)
		return wagering.LedgerRow{Seq: seq, Entry: e}, err
	})
}

// Totals sums in SQL with numeric to rule out bigint overflow of the sum
// itself; the result is converted back to minor units only if it fits.
func (r ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID, currency money.Currency) (money.Money, int64, error) {
	var total string
	var n int64
	err := r.tx.QueryRow(ctx, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor::numeric
			ELSE -amount_minor::numeric END), 0)::text, count(*)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&total, &n)
	if err != nil {
		return money.Money{}, 0, err
	}
	var minor int64
	if _, err := fmt.Sscan(total, &minor); err != nil {
		return money.Money{}, 0, fmt.Errorf("ledger total %s: %w", total, money.ErrOverflow)
	}
	m, err := money.FromMinor(minor, currency)
	return m, n, err
}

// ---- outbox ----

type outboxRepo struct{ tx pgx.Tx }

func (r outboxRepo) Append(ctx context.Context, evs ...events.Event) error {
	if len(evs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range evs {
		batch.Queue(`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload, occurred_at, next_attempt_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$7)`, e.ID, events.AggregateWallet, e.AggregateID, e.Type, e.Version, e.Payload, e.OccurredAt)
	}
	return r.tx.SendBatch(ctx, batch).Close()
}

// ---- inbox ----

type inboxRepo struct{ tx pgx.Tx }

// Register inserts the message or, if it exists, reads it. A concurrent
// delivery of the same message waits on the primary key until the first one
// commits or rolls back, then sees its outcome.
func (r inboxRepo) Register(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (wagering.InboxState, error) {
	tag, err := r.tx.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1,$2,$3,$4) ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumer, messageID, payloadHash, now)
	if err != nil {
		return wagering.InboxState{}, err
	}
	if tag.RowsAffected() == 1 {
		return wagering.InboxState{New: true, HashMatches: true}, nil
	}
	var (
		hash      string
		txID      uuid.NullUUID
		completed *time.Time
	)
	if err := r.tx.QueryRow(ctx, `SELECT payload_hash, transaction_id, completed_at FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&hash, &txID, &completed); err != nil {
		return wagering.InboxState{}, err
	}
	return wagering.InboxState{HashMatches: hash == payloadHash, TransactionID: txID.UUID, Completed: completed != nil}, nil
}

func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error {
	tag, err := r.tx.Exec(ctx, `UPDATE inbox_messages SET transaction_id = $3, completed_at = $4
		WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NULL`, consumer, messageID, transactionID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: inbox message %s already completed", wagering.ErrDuplicate, messageID)
	}
	return nil
}
