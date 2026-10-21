//go:build integration

package migrations_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/platform/postgres"
	"github.com/devfertonani/wallet-service/internal/testsupport/pgtest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fixture writes a consistent wallet with an OPENING of 100.00 BRL, the way
// the application will: wallet, transaction and ledger entry in one commit.
type fixture struct {
	walletID, playerID, openingID uuid.UUID
}

func openWallet(t *testing.T, pool *pgxpool.Pool) fixture {
	t.Helper()
	f := fixture{walletID: uuid.New(), playerID: uuid.New(), openingID: uuid.New()}
	now := time.Now().UTC()
	tx(t, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx(), `INSERT INTO wallets VALUES ($1,$2,'BRL',10000,1,$3,$3)`, f.walletID, f.playerID, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency, result_balance_minor, created_at, updated_at)
			VALUES ($1,'INTERNAL','OPENING','PROCESSED',$2,$3,10000,'BRL',10000,$4,$4)`, f.openingID, f.walletID, f.playerID, now); err != nil {
			return err
		}
		_, err := tx.Exec(ctx(), `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
			VALUES ($1,$2,$3,'CREDIT',10000,'BRL',0,10000,1,$4)`, uuid.New(), f.walletID, f.openingID, now)
		return err
	})
	return f
}

func insertBet(t *testing.T, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, f fixture, extID, status string) (uuid.UUID, error) {
	id := uuid.New()
	now := time.Now().UTC()
	var result any
	var failure any
	switch status {
	case "PROCESSED":
		result = 2000
	case "REJECTED":
		failure = "INSUFFICIENT_FUNDS"
	}
	_, err := q.Exec(ctx(), `INSERT INTO wager_transactions
		(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		 result_balance_minor, failure_code, created_at, updated_at)
		VALUES ($1,'EXTERNAL','BET',$2,$3,$4,8000,'BRL','provider-a',$5,$6,'h','r1','g1',$7,$8,$9,$9)`,
		id, status, f.walletID, f.playerID, extID, "provider-a:"+extID, result, failure, now)
	return id, err
}

// move records a processed external operation the way the application does:
// transaction, ledger entry and wallet update in one commit, under the wallet
// row lock.
func move(pool *pgxpool.Pool, f fixture, kind, extID string, amount int64, dir string, ref *uuid.UUID) (uuid.UUID, error) {
	id := uuid.New()
	err := txErr(pool, func(tx pgx.Tx) error {
		var balance, version int64
		if err := tx.QueryRow(ctx(), `SELECT balance_minor, version FROM wallets WHERE id = $1 FOR UPDATE`, f.walletID).Scan(&balance, &version); err != nil {
			return err
		}
		after := balance - amount
		if dir == "CREDIT" {
			after = balance + amount
		}
		var refExt any
		if ref != nil {
			if err := tx.QueryRow(ctx(), `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, *ref).Scan(&refExt); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 reference_external_transaction_id, reference_transaction_id, result_balance_minor, created_at, updated_at)
			VALUES ($1,'EXTERNAL',$2,'PROCESSED',$3,$4,$5,'BRL','provider-a',$6,$7,'h','r1','g1',$8,$9,$10,now(),now())`,
			id, kind, f.walletID, f.playerID, amount, extID, "provider-a:"+extID, refExt, ref, after); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx(), `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
			VALUES ($1,$2,$3,$4,$5,'BRL',$6,$7,$8,now())`, uuid.New(), f.walletID, id, dir, amount, balance, after, version+1); err != nil {
			return err
		}
		_, err := tx.Exec(ctx(), `UPDATE wallets SET balance_minor = $2, version = $3, updated_at = now() WHERE id = $1`, f.walletID, after, version+1)
		return err
	})
	return id, err
}

func ctx() context.Context { return context.Background() }

func tx(t *testing.T, pool *pgxpool.Pool, fn func(pgx.Tx) error) {
	t.Helper()
	if err := txErr(pool, fn); err != nil {
		t.Fatal(err)
	}
}

func txErr(pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx(), pool, fn)
}

func wantPgError(t *testing.T, err error, code string, contains string) {
	t.Helper()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("want postgres error %s, got %v", code, err)
	}
	if pe.Code != code {
		t.Fatalf("want SQLSTATE %s, got %s: %s", code, pe.Code, pe.Message)
	}
	if contains != "" && !strings.Contains(pe.Message+pe.ConstraintName, contains) {
		t.Fatalf("want error mentioning %q, got %q (constraint %q)", contains, pe.Message, pe.ConstraintName)
	}
}

const (
	checkViolation     = "23514"
	uniqueViolation    = "23505"
	integrityViolation = "23000"
	insufficientPriv   = "42501"
)

func TestMigrationsRevertAndReapply(t *testing.T) {
	db := pgtest.New(t)
	m, err := postgres.NewMigrator(db.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	if _, _, err := m.Version(); !errors.Is(err, migrate.ErrNilVersion) {
		t.Fatalf("after down, version err = %v", err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	v, dirty, err := m.Version()
	if err != nil || dirty || v != 4 {
		t.Fatalf("version=%d dirty=%t err=%v", v, dirty, err)
	}
}

func TestWalletInvariants(t *testing.T) {
	db := pgtest.New(t)
	f := openWallet(t, db.App)

	t.Run("one wallet per player and currency", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `INSERT INTO wallets VALUES ($1,$2,'BRL',0,1,now(),now())`, uuid.New(), f.playerID)
		wantPgError(t, err, uniqueViolation, "wallets_player_currency_key")
	})
	t.Run("balance cannot go negative", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1`, f.walletID)
		wantPgError(t, err, checkViolation, "balance_minor")
	})
	t.Run("balance change without ledger entry fails at commit", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx(), `UPDATE wallets SET balance_minor = 5000, version = version + 1 WHERE id = $1`, f.walletID)
			return err
		})
		wantPgError(t, err, integrityViolation, "without a matching ledger entry")
	})
	t.Run("balance change without version increment fails", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx(), `UPDATE wallets SET balance_minor = 5000 WHERE id = $1`, f.walletID)
			return err
		})
		wantPgError(t, err, integrityViolation, "without a version increment")
	})
	t.Run("opening balance without ledger entry fails at commit", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx(), `INSERT INTO wallets VALUES ($1,$2,'BRL',500,1,now(),now())`, uuid.New(), uuid.New())
			return err
		})
		wantPgError(t, err, integrityViolation, "without its OPENING ledger entry")
	})
	t.Run("zero opening needs no ledger", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `INSERT INTO wallets VALUES ($1,$2,'BRL',0,1,now(),now())`, uuid.New(), uuid.New())
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wallets cannot be deleted", func(t *testing.T) {
		_, err := db.Owner.Exec(ctx(), `DELETE FROM wallets WHERE id = $1`, f.walletID)
		wantPgError(t, err, integrityViolation, "append-only")
	})
}

func TestLedgerIsAppendOnlyAndConsistent(t *testing.T) {
	db := pgtest.New(t)
	f := openWallet(t, db.App)

	t.Run("app role has no UPDATE or DELETE privilege", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `UPDATE wallet_ledger_entries SET amount_minor = 1`)
		wantPgError(t, err, insufficientPriv, "")
		_, err = db.App.Exec(ctx(), `DELETE FROM wallet_ledger_entries`)
		wantPgError(t, err, insufficientPriv, "")
		_, err = db.App.Exec(ctx(), `TRUNCATE wallet_ledger_entries`)
		wantPgError(t, err, insufficientPriv, "")
	})
	t.Run("even the owner cannot change or delete entries", func(t *testing.T) {
		_, err := db.Owner.Exec(ctx(), `UPDATE wallet_ledger_entries SET amount_minor = 1`)
		wantPgError(t, err, integrityViolation, "append-only")
		_, err = db.Owner.Exec(ctx(), `DELETE FROM wallet_ledger_entries`)
		wantPgError(t, err, integrityViolation, "append-only")
		_, err = db.Owner.Exec(ctx(), `TRUNCATE wallet_ledger_entries CASCADE`)
		wantPgError(t, err, integrityViolation, "append-only")
	})
	t.Run("balance arithmetic is checked", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			betID, err := insertBet(t, tx, f, "arith", "PROCESSED")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx(), `INSERT INTO wallet_ledger_entries
				(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
				VALUES ($1,$2,$3,'DEBIT',8000,'BRL',10000,3000,2,now())`, uuid.New(), f.walletID, betID)
			return err
		})
		wantPgError(t, err, checkViolation, "wallet_ledger_entries_arithmetic")
	})
	t.Run("one entry per wallet and transaction", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
			VALUES ($1,$2,$3,'CREDIT',10000,'BRL',0,10000,9,now())`, uuid.New(), f.walletID, f.openingID)
		wantPgError(t, err, uniqueViolation, "wallet_ledger_entries_wallet_transaction_key")
	})
	t.Run("entry currency must match the wallet", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			betID, err := insertBet(t, tx, f, "currency", "PROCESSED")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx(), `INSERT INTO wallet_ledger_entries
				(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
				VALUES ($1,$2,$3,'DEBIT',8000,'USD',10000,2000,2,now())`, uuid.New(), f.walletID, betID)
			return err
		})
		wantPgError(t, err, "23503", "_fk")
	})
}

func TestWagerTransactionConstraints(t *testing.T) {
	db := pgtest.New(t)
	f := openWallet(t, db.App)

	t.Run("one operation per provider and external id", func(t *testing.T) {
		if _, err := insertBet(t, db.App, f, "dup", "REJECTED"); err != nil {
			t.Fatal(err)
		}
		_, err := db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 failure_code, created_at, updated_at)
			VALUES ($1,'EXTERNAL','BET','REJECTED',$2,$3,8000,'BRL','provider-a','dup','another-key','h','r1','g1','INSUFFICIENT_FUNDS',now(),now())`,
			uuid.New(), f.walletID, f.playerID)
		wantPgError(t, err, uniqueViolation, "wager_transactions_provider_external_key")
	})
	t.Run("same external id is fine for another provider", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 failure_code, created_at, updated_at)
			VALUES ($1,'EXTERNAL','BET','REJECTED',$2,$3,8000,'BRL','provider-b','dup','provider-b:dup','h','r1','g1','INSUFFICIENT_FUNDS',now(),now())`,
			uuid.New(), f.walletID, f.playerID)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("OPENING is internal only and unique per wallet", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 result_balance_minor, created_at, updated_at)
			VALUES ($1,'EXTERNAL','OPENING','PROCESSED',$2,$3,8000,'BRL','provider-a','op','provider-a:op','h','r1','g1',0,now(),now())`,
			uuid.New(), f.walletID, f.playerID)
		wantPgError(t, err, checkViolation, "wager_transactions_origin_shape")
		_, err = db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency, result_balance_minor, created_at, updated_at)
			VALUES ($1,'INTERNAL','OPENING','PROCESSED',$2,$3,10000,'BRL',10000,now(),now())`, uuid.New(), f.walletID, f.playerID)
		wantPgError(t, err, uniqueViolation, "wager_transactions_one_opening_per_wallet")
	})
	t.Run("zero amount policy", func(t *testing.T) {
		_, err := db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 result_balance_minor, created_at, updated_at)
			VALUES ($1,'EXTERNAL','LOSS','PROCESSED',$2,$3,100,'BRL','provider-a','loss','provider-a:loss','h','r1','g1',0,now(),now())`,
			uuid.New(), f.walletID, f.playerID)
		wantPgError(t, err, checkViolation, "wager_transactions_amount_policy")
		_, err = db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 result_balance_minor, created_at, updated_at)
			VALUES ($1,'EXTERNAL','BET','PROCESSED',$2,$3,0,'BRL','provider-a','zero','provider-a:zero','h','r1','g1',0,now(),now())`,
			uuid.New(), f.walletID, f.playerID)
		wantPgError(t, err, checkViolation, "wager_transactions_amount_policy")
	})
	t.Run("a reference is compensated at most once", func(t *testing.T) {
		betID, err := move(db.App, f, "BET", "to-reverse", 2000, "DEBIT", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := move(db.App, f, "REFUND", "refund-1", 2000, "CREDIT", &betID); err != nil {
			t.Fatal(err)
		}
		_, err = move(db.App, f, "REFUND", "refund-2", 2000, "CREDIT", &betID)
		wantPgError(t, err, uniqueViolation, "wager_transactions_one_reversal_per_reference")
		_, err = move(db.App, f, "ROLLBACK", "rollback-1", 2000, "CREDIT", &betID)
		wantPgError(t, err, uniqueViolation, "wager_transactions_one_reversal_per_reference")
	})
	t.Run("terminal transactions are frozen", func(t *testing.T) {
		id, err := insertBet(t, db.App, f, "frozen", "REJECTED")
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.App.Exec(ctx(), `UPDATE wager_transactions SET status = 'PROCESSED', failure_code = NULL, result_balance_minor = 1 WHERE id = $1`, id)
		wantPgError(t, err, integrityViolation, "is terminal")
		_, err = db.Owner.Exec(ctx(), `DELETE FROM wager_transactions WHERE id = $1`, id)
		wantPgError(t, err, integrityViolation, "append-only")
	})
	t.Run("financial fields of a pending transaction cannot change", func(t *testing.T) {
		id := uuid.New()
		_, err := db.App.Exec(ctx(), `INSERT INTO wager_transactions
			(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			 reference_external_transaction_id, next_attempt_at, expires_at, created_at, updated_at)
			VALUES ($1,'EXTERNAL','REFUND','PENDING_REFERENCE',$2,$3,8000,'BRL','provider-a','pending','provider-a:pending','h','r1','g1','missing',now(),now() + interval '1 hour',now(),now())`,
			id, f.walletID, f.playerID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.App.Exec(ctx(), `UPDATE wager_transactions SET amount_minor = 1 WHERE id = $1`, id)
		wantPgError(t, err, integrityViolation, "immutable fields")
		if _, err = db.App.Exec(ctx(), `UPDATE wager_transactions SET attempts = attempts + 1 WHERE id = $1`, id); err != nil {
			t.Fatalf("retry bookkeeping must stay writable: %v", err)
		}
		_, err = db.App.Exec(ctx(), `UPDATE wager_transactions SET attempts = 0 WHERE id = $1`, id)
		wantPgError(t, err, integrityViolation, "cannot decrease")
		_, err = db.App.Exec(ctx(), `UPDATE wager_transactions SET status = 'PENDING' WHERE id = $1`, id)
		wantPgError(t, err, integrityViolation, "PENDING_REFERENCE to PENDING")
	})
}

// TestLedgerIsBoundToItsTransaction reproduces the attacks of the first review
// (A1, A3, A4): each one was accepted by the schema before 000003.
func TestLedgerIsBoundToItsTransaction(t *testing.T) {
	db := pgtest.New(t)
	f := openWallet(t, db.App)
	other := openWallet(t, db.App)
	entry := func(tx pgx.Tx, walletID, txID uuid.UUID, dir string, amount, before, after, version int64) error {
		_, err := tx.Exec(ctx(), `INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_version, created_at)
			VALUES ($1,$2,$3,$4,$5,'BRL',$6,$7,$8,now())`, uuid.New(), walletID, txID, dir, amount, before, after, version)
		return err
	}

	t.Run("A1: entry without balance change, for a transaction of another wallet", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			return entry(tx, f.walletID, other.openingID, "CREDIT", 10000, 10000, 20000, 2)
		})
		wantPgError(t, err, "23503", "wallet_ledger_entries_transaction_binding_fk")
	})
	t.Run("A1: entry for its own transaction without updating the wallet", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			id, err := insertBet(t, tx, f, "no-wallet-update", "PROCESSED")
			if err != nil {
				return err
			}
			return entry(tx, f.walletID, id, "DEBIT", 8000, 10000, 2000, 2)
		})
		wantPgError(t, err, integrityViolation, "not reflected in wallet")
	})
	t.Run("A3: entry for a rejected transaction", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			id, err := insertBet(t, tx, f, "rejected-with-entry", "REJECTED")
			if err != nil {
				return err
			}
			return entry(tx, f.walletID, id, "DEBIT", 8000, 10000, 2000, 2)
		})
		wantPgError(t, err, integrityViolation, "in status REJECTED")
	})
	t.Run("A4: BET producing a credit", func(t *testing.T) {
		_, err := move(db.App, f, "BET", "bet-as-credit", 2000, "CREDIT", nil)
		wantPgError(t, err, integrityViolation, "direction CREDIT for a BET")
	})
	t.Run("A4: entry amount different from the transaction", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			id, err := insertBet(t, tx, f, "other-amount", "PROCESSED")
			if err != nil {
				return err
			}
			return entry(tx, f.walletID, id, "DEBIT", 100, 10000, 9900, 2)
		})
		wantPgError(t, err, "23503", "wallet_ledger_entries_transaction_binding_fk")
	})
	t.Run("processed BET without its entry fails at commit", func(t *testing.T) {
		_, err := insertBet(t, db.App, f, "processed-without-entry", "PROCESSED")
		wantPgError(t, err, integrityViolation, "has no matching ledger entry")
	})
	t.Run("entry that breaks the chain", func(t *testing.T) {
		err := txErr(db.App, func(tx pgx.Tx) error {
			id, err := insertBet(t, tx, f, "broken-chain", "PROCESSED")
			if err != nil {
				return err
			}
			// Claims the wallet was at 90.00 before; it was at 100.00.
			if err := entry(tx, f.walletID, id, "DEBIT", 8000, 9000, 1000, 2); err != nil {
				return err
			}
			_, err = tx.Exec(ctx(), `UPDATE wallets SET balance_minor = 1000, version = 2 WHERE id = $1`, f.walletID)
			return err
		})
		wantPgError(t, err, integrityViolation, "")
	})
	t.Run("a coherent movement is accepted", func(t *testing.T) {
		betID, err := move(db.App, f, "BET", "ok-bet", 2500, "DEBIT", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := move(db.App, f, "ROLLBACK", "ok-rollback", 2500, "CREDIT", &betID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCompletedInboxMessageIsFrozen(t *testing.T) {
	db := pgtest.New(t)
	_, err := db.App.Exec(ctx(), `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ('c', 'm-1', 'h1', now())`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.App.Exec(ctx(), `UPDATE inbox_messages SET completed_at = now() WHERE message_id = 'm-1'`); err != nil {
		t.Fatalf("completing a message must be allowed: %v", err)
	}
	_, err = db.App.Exec(ctx(), `UPDATE inbox_messages SET completed_at = NULL WHERE message_id = 'm-1'`)
	wantPgError(t, err, integrityViolation, "immutable")
	_, err = db.App.Exec(ctx(), `UPDATE inbox_messages SET payload_hash = 'h2' WHERE message_id = 'm-1'`)
	wantPgError(t, err, integrityViolation, "immutable")
	_, err = db.Owner.Exec(ctx(), `DELETE FROM inbox_messages WHERE message_id = 'm-1'`)
	wantPgError(t, err, integrityViolation, "append-only")
}

func TestOutboxPayloadIsImmutable(t *testing.T) {
	db := pgtest.New(t)
	id := uuid.New()
	_, err := db.App.Exec(ctx(), `INSERT INTO outbox_events
		(id, aggregate_type, aggregate_id, event_type, event_version, payload, occurred_at, next_attempt_at)
		VALUES ($1,'Wallet',$2,'WalletBalanceChanged',1,'{"a":1}',now(),now())`, id, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.App.Exec(ctx(), `UPDATE outbox_events SET payload = '{"a":2}' WHERE id = $1`, id)
	wantPgError(t, err, integrityViolation, "immutable")
	if _, err := db.App.Exec(ctx(), `UPDATE outbox_events SET attempts = 1, published_at = now() WHERE id = $1`, id); err != nil {
		t.Fatalf("delivery bookkeeping must stay writable: %v", err)
	}
	_, err = db.App.Exec(ctx(), `UPDATE outbox_events SET published_at = NULL WHERE id = $1`, id)
	wantPgError(t, err, integrityViolation, "immutable")
	_, err = db.App.Exec(ctx(), `DELETE FROM outbox_events WHERE id = $1`, id)
	wantPgError(t, err, insufficientPriv, "")
}

// Second review (C1, A1, M3).
func TestReviewRegressions(t *testing.T) {
	db := pgtest.New(t)
	f := openWallet(t, db.App)

	t.Run("a wallet opened with zero accepts its first movement at version 2", func(t *testing.T) {
		zero := fixture{walletID: uuid.New(), playerID: uuid.New()}
		if _, err := db.App.Exec(ctx(), `INSERT INTO wallets VALUES ($1,$2,'BRL',0,1,now(),now())`, zero.walletID, zero.playerID); err != nil {
			t.Fatal(err)
		}
		if _, err := move(db.App, zero, "WIN", "first-win", 100, "CREDIT", nil); err != nil {
			t.Fatalf("first credit of a zero wallet: %v", err)
		}
		if _, err := move(db.App, zero, "BET", "then-bet", 40, "DEBIT", nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a currency mismatch rejection is recordable, nothing else in another currency is", func(t *testing.T) {
		insert := func(status, failure string, result any) error {
			_, err := db.App.Exec(ctx(), `INSERT INTO wager_transactions
				(id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
				 provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
				 failure_code, result_balance_minor, created_at, updated_at)
				VALUES ($1,'EXTERNAL','LOSS',$2,$3,$4,0,'USD','provider-a',$5,$5,'h','r1','g1',$6,$7,now(),now())`,
				uuid.New(), status, f.walletID, f.playerID, "usd-"+status+failure, failure, result)
			return err
		}
		if err := insert("REJECTED", "CURRENCY_MISMATCH", nil); err != nil {
			t.Fatalf("currency mismatch rejection: %v", err)
		}
		wantPgError(t, insert("REJECTED", "INSUFFICIENT_FUNDS", nil), integrityViolation, "does not match the currency")
		wantPgError(t, insert("PROCESSED", "", 0), integrityViolation, "does not match the currency")
	})
	t.Run("a processed reversal must name its reference", func(t *testing.T) {
		if _, err := move(db.App, f, "BET", "bet-30", 3000, "DEBIT", nil); err != nil {
			t.Fatal(err)
		}
		_, err := move(db.App, f, "REFUND", "refund-without-ref", 3000, "CREDIT", nil)
		wantPgError(t, err, checkViolation, "wager_transactions_processed_reversal_resolved")
	})
	t.Run("a reversal must have the referenced amount", func(t *testing.T) {
		betID, err := move(db.App, f, "BET", "bet-small", 3000, "DEBIT", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = move(db.App, f, "REFUND", "refund-huge", 99999, "CREDIT", &betID)
		wantPgError(t, err, integrityViolation, "differs from the referenced amount")
	})
	t.Run("a reversal cannot reference a disallowed kind", func(t *testing.T) {
		winID, err := move(db.App, f, "WIN", "win-x", 500, "CREDIT", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = move(db.App, f, "REFUND", "refund-of-win", 500, "CREDIT", &winID)
		wantPgError(t, err, integrityViolation, "REFUND cannot reference a WIN")
	})
}
