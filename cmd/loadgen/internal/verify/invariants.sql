-- Post-run financial invariants, checked over the WHOLE database (not only
-- the run's wallets). Every query lists violating rows: an empty result
-- means the invariant holds. `loadgen verify` runs them in one
-- REPEATABLE READ READ ONLY transaction; they also run as is in psql:
--   psql "$LOADGEN_DATABASE_URL" -f cmd/loadgen/internal/verify/invariants.sql
-- Several of them are also enforced by constraints and triggers
-- (migrations 000001 and 000003); checking again after the fact is the audit
-- that proves the constraints held under load, not a substitute for them.

-- name: balance_equals_ledger
-- description: stored balance = sum of credits − sum of debits in the ledger
SELECT w.id, w.balance_minor AS stored, coalesce(l.sum, 0) AS ledger
FROM wallets w
LEFT JOIN (
    SELECT wallet_id, sum(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END) AS sum
    FROM wallet_ledger_entries GROUP BY wallet_id
) l ON l.wallet_id = w.id
WHERE w.balance_minor <> coalesce(l.sum, 0);

-- name: version_equals_last_entry
-- description: wallet version = highest wallet_version in the ledger (1 when there is no entry)
SELECT w.id, w.version, l.max_version
FROM wallets w
LEFT JOIN (SELECT wallet_id, max(wallet_version) AS max_version FROM wallet_ledger_entries GROUP BY wallet_id) l
    ON l.wallet_id = w.id
WHERE w.version <> coalesce(l.max_version, 1)
   OR (l.max_version IS NOT NULL AND w.balance_minor <> (
        SELECT balance_after FROM wallet_ledger_entries e
        WHERE e.wallet_id = w.id AND e.wallet_version = l.max_version));

-- name: ledger_chain_continuity
-- description: ledger chain without gaps: version n = (n−1) + 1 and balance_before(n) = balance_after(n−1)
SELECT wallet_id, wallet_version, prev_version, balance_before, prev_after
FROM (
    SELECT wallet_id, wallet_version, balance_before,
           lag(wallet_version) OVER w AS prev_version,
           lag(balance_after) OVER w AS prev_after
    FROM wallet_ledger_entries
    WINDOW w AS (PARTITION BY wallet_id ORDER BY wallet_version)
) c
WHERE (prev_version IS NULL AND wallet_version = 1 AND balance_before <> 0)
   OR (prev_version IS NOT NULL AND (wallet_version <> prev_version + 1 OR balance_before <> prev_after));

-- name: no_negative_balance
-- description: no negative balance, in the wallet or at any point of the ledger
SELECT 'wallet' AS what, id::text, balance_minor FROM wallets WHERE balance_minor < 0
UNION ALL
SELECT 'ledger', id::text, balance_after FROM wallet_ledger_entries WHERE balance_after < 0 OR balance_before < 0;

-- name: no_duplicate_operation
-- description: a single transaction per (provider, externalTransactionId) and per (provider, Idempotency-Key)
SELECT 'external_id' AS what, provider_id, external_transaction_id AS value, count(*)
FROM wager_transactions WHERE origin = 'EXTERNAL'
GROUP BY provider_id, external_transaction_id HAVING count(*) > 1
UNION ALL
SELECT 'idempotency_key', provider_id, idempotency_key, count(*)
FROM wager_transactions WHERE origin = 'EXTERNAL'
GROUP BY provider_id, idempotency_key HAVING count(*) > 1;

-- name: processed_moving_has_one_entry
-- description: every PROCESSED transaction that moves the balance has exactly one entry, with the same amount and resulting balance
SELECT t.id, t.kind, count(e.id) AS entries, t.amount_minor, min(e.amount_minor) AS entry_amount,
       t.result_balance_minor, min(e.balance_after) AS entry_after
FROM wager_transactions t
LEFT JOIN wallet_ledger_entries e ON e.transaction_id = t.id
WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS'
GROUP BY t.id
HAVING count(e.id) <> 1 OR min(e.amount_minor) <> t.amount_minor OR min(e.balance_after) <> t.result_balance_minor;

-- name: non_moving_has_no_entry
-- description: LOSS, REJECTED, FAILED and pending transactions have no entry
SELECT t.id, t.kind, t.status, count(*) AS entries
FROM wager_transactions t
JOIN wallet_ledger_entries e ON e.transaction_id = t.id
WHERE t.kind = 'LOSS' OR t.status <> 'PROCESSED'
GROUP BY t.id;

-- name: entry_direction_matches_kind
-- description: entry direction matches the kind (ROLLBACK: opposite to the reference)
SELECT e.id, t.kind, e.direction, r.kind AS reference_kind
FROM wallet_ledger_entries e
JOIN wager_transactions t ON t.id = e.transaction_id
LEFT JOIN wager_transactions r ON r.id = t.reference_transaction_id
WHERE e.direction IS DISTINCT FROM CASE t.kind
    WHEN 'OPENING' THEN 'CREDIT' WHEN 'WIN' THEN 'CREDIT' WHEN 'REFUND' THEN 'CREDIT' WHEN 'BET' THEN 'DEBIT'
    WHEN 'ROLLBACK' THEN CASE r.kind WHEN 'BET' THEN 'CREDIT' WHEN 'WIN' THEN 'DEBIT' WHEN 'REFUND' THEN 'DEBIT' END
END;

-- name: one_successful_reversal_per_reference
-- description: at most one PROCESSED reversal (REFUND or ROLLBACK) per reference
SELECT reference_transaction_id, count(*)
FROM wager_transactions
WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK')
GROUP BY reference_transaction_id HAVING count(*) > 1;

-- name: reversal_matches_reference
-- description: a processed reversal has the same wallet and amount as its reference
SELECT t.id, t.kind, t.amount_minor, r.amount_minor AS reference_amount, t.wallet_id, r.wallet_id AS reference_wallet
FROM wager_transactions t
JOIN wager_transactions r ON r.id = t.reference_transaction_id
WHERE t.status = 'PROCESSED' AND t.kind IN ('REFUND', 'ROLLBACK')
  AND (t.amount_minor <> r.amount_minor OR t.wallet_id <> r.wallet_id);

-- name: no_failed_transactions
-- description: no FAILED transaction (e.g. INVARIANT_VIOLATION) and no persisted PENDING
SELECT id, kind, status, failure_code FROM wager_transactions WHERE status IN ('FAILED', 'PENDING');

-- name: no_stuck_pending_reference
-- description: no PENDING_REFERENCE expired for more than 30 s (the worker should have rejected it)
SELECT id, kind, expires_at, attempts FROM wager_transactions
WHERE status = 'PENDING_REFERENCE' AND expires_at < now() - interval '30 seconds';

-- name: outbox_fully_published
-- description: outbox drained: no event without published_at
SELECT id, event_type, occurred_at, attempts, last_error FROM outbox_events WHERE published_at IS NULL;

-- name: outbox_events_match_state
-- description: one WagerTransactionProcessed per PROCESSED, one WalletBalanceChanged per entry, one WagerTransactionRejected per REJECTED
SELECT x.event_type, x.expected, coalesce(o.actual, 0) AS actual
FROM (
    SELECT 'WagerTransactionProcessed' AS event_type, count(*) AS expected FROM wager_transactions WHERE status = 'PROCESSED'
    UNION ALL SELECT 'WalletBalanceChanged', count(*) FROM wallet_ledger_entries
    UNION ALL SELECT 'WagerTransactionRejected', count(*) FROM wager_transactions WHERE status = 'REJECTED'
) x
LEFT JOIN (SELECT event_type, count(*) AS actual FROM outbox_events GROUP BY event_type) o ON o.event_type = x.event_type
WHERE x.expected <> coalesce(o.actual, 0);

-- name: inbox_all_completed
-- description: every received SQS message was completed
SELECT consumer_name, message_id, received_at FROM inbox_messages WHERE completed_at IS NULL;
