// Package pgstore implements the wagering ports on PostgreSQL with pgx and
// explicit SQL. Locks, isolation levels and constraint names are visible here.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UnitOfWork struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

var _ wagering.UnitOfWork = (*UnitOfWork)(nil)

func (u *UnitOfWork) Run(ctx context.Context, fn func(context.Context, wagering.Repos) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

func (u *UnitOfWork) Read(ctx context.Context, fn func(context.Context, wagering.Repos) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, opts pgx.TxOptions, fn func(context.Context, wagering.Repos) error) error {
	tx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after commit
	if err := fn(ctx, repos(tx)); err != nil {
		return classify(err)
	}
	// Deferred constraint triggers (ledger binding, balance has ledger) run
	// here, so commit errors are classified like statement errors.
	return classify(tx.Commit(ctx))
}

func repos(tx pgx.Tx) wagering.Repos {
	return wagering.Repos{
		Wallets:      walletRepo{tx},
		Transactions: transactionRepo{tx},
		Ledger:       ledgerRepo{tx},
		Outbox:       outboxRepo{tx},
		Inbox:        inboxRepo{tx},
	}
}

// Unique constraints that mean "someone else got there first" rather than a
// broken invariant.
var duplicateConstraints = map[string]error{
	"wallets_player_currency_key":                 wagering.ErrWalletExists,
	"wager_transactions_provider_external_key":    wagering.ErrDuplicate,
	"wager_transactions_provider_idempotency_key": wagering.ErrDuplicate,
	"inbox_messages_pkey":                         wagering.ErrDuplicate,
}

// classify maps database errors onto the port errors. Errors that are already
// classified, and domain errors, pass through unchanged.
func classify(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{wagering.ErrTransient, wagering.ErrIntegrity, wagering.ErrDuplicate, wagering.ErrWalletExists,
		wagering.ErrNotFound, wagering.ErrConcurrentUpdate} {
		if errors.Is(err, known) {
			return err
		}
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch {
		case pe.Code == "23505":
			if mapped, ok := duplicateConstraints[pe.ConstraintName]; ok {
				return fmt.Errorf("%w: %s", mapped, pe.ConstraintName)
			}
			return fmt.Errorf("%w: %w", wagering.ErrIntegrity, err)
		case strings.HasPrefix(pe.Code, "23"):
			return fmt.Errorf("%w: %w", wagering.ErrIntegrity, err)
		case pe.Code == "55P03" || pe.Code == "40P01": // lock_timeout, deadlock
			return fmt.Errorf("%w: %w: %w", wagering.ErrTransient, wagering.ErrLockConflict, err)
		case isTransientCode(pe.Code):
			return fmt.Errorf("%w: %w", wagering.ErrTransient, err)
		}
		return err
	}
	var netErr net.Error
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) || errors.As(err, &netErr) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, pgx.ErrTxClosed) ||
		strings.Contains(err.Error(), "conn closed") || strings.Contains(err.Error(), "closed pool") {
		return fmt.Errorf("%w: %w", wagering.ErrTransient, err)
	}
	return err
}

func isTransientCode(code string) bool {
	switch code {
	case "40001", // serialization_failure
		"40P01",                   // deadlock_detected
		"55P03",                   // lock_not_available (lock_timeout)
		"57014",                   // query_canceled (statement_timeout)
		"57P01", "57P02", "57P03", // admin shutdown, crash shutdown, cannot connect now
		"53300": // too_many_connections
		return true
	}
	return strings.HasPrefix(code, "08") // connection exceptions
}
