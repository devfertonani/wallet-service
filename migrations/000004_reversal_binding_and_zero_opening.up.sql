-- Fixes and hardening after the second review:
--   * a wallet opened with 0.00 has no OPENING entry, so its first entry is
--     version 2 starting at zero; 000003 refused it;
--   * a CURRENCY_MISMATCH rejection must be recordable, but the composite FK
--     (wallet_id, currency) refused any row in another currency. Currency
--     equality is kept for every row that is not a rejection, by trigger; the
--     ledger keeps its own composite FKs, so money never moves in another
--     currency;
--   * a PROCESSED REFUND/ROLLBACK (and a WIN with a reference) is bound to its
--     reference: resolved, PROCESSED, same wallet, player, provider and round,
--     allowed kind, and the full amount for reversals.

-- Ledger chain: the first entry of a wallet starts at zero, at version 1
-- (opened with a balance) or 2 (opened with zero, first movement).
CREATE OR REPLACE FUNCTION wallet_ledger_entries_chain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    w_version bigint;
    w_balance bigint;
    prev_version bigint;
    prev_after bigint;
BEGIN
    SELECT wallet_version, balance_after INTO prev_version, prev_after
    FROM wallet_ledger_entries
    WHERE wallet_id = NEW.wallet_id AND wallet_version < NEW.wallet_version
    ORDER BY wallet_version DESC LIMIT 1;
    IF prev_version IS NULL THEN
        IF NEW.balance_before <> 0 OR NEW.wallet_version NOT IN (1, 2) THEN
            RAISE EXCEPTION 'first ledger entry of wallet % must start at zero at version 1 or 2', NEW.wallet_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF prev_version <> NEW.wallet_version - 1 OR prev_after <> NEW.balance_before THEN
        RAISE EXCEPTION 'ledger entry % breaks the chain of wallet % at version %', NEW.id, NEW.wallet_id, NEW.wallet_version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    SELECT version, balance_minor INTO w_version, w_balance FROM wallets WHERE id = NEW.wallet_id;
    IF w_version < NEW.wallet_version
        OR (w_version = NEW.wallet_version AND w_balance <> NEW.balance_after) THEN
        RAISE EXCEPTION 'ledger entry % is not reflected in wallet %', NEW.id, NEW.wallet_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

-- Currency of a transaction: equal to the wallet's unless it is the rejection
-- that reports the difference.
ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_wallet_fk;
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_wallet_id_fk FOREIGN KEY (wallet_id) REFERENCES wallets (id);

CREATE FUNCTION wager_transactions_currency() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'REJECTED' AND NEW.failure_code = 'CURRENCY_MISMATCH' THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM wallets WHERE id = NEW.wallet_id AND currency = NEW.currency) THEN
        RAISE EXCEPTION 'wager transaction % in % does not match the currency of wallet %', NEW.id, NEW.currency, NEW.wallet_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_currency
    BEFORE INSERT OR UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_currency();

-- A processed reversal always names the transaction it compensates.
ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_processed_reversal_resolved CHECK (
    status <> 'PROCESSED' OR kind NOT IN ('REFUND', 'ROLLBACK') OR reference_transaction_id IS NOT NULL
);

CREATE FUNCTION wager_transactions_reference_binding() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    IF NEW.status <> 'PROCESSED' OR NEW.reference_transaction_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO r FROM wager_transactions WHERE id = NEW.reference_transaction_id;
    IF r.status IS DISTINCT FROM 'PROCESSED'
        OR r.wallet_id <> NEW.wallet_id OR r.player_id <> NEW.player_id
        OR r.provider_id IS DISTINCT FROM NEW.provider_id OR r.round_id IS DISTINCT FROM NEW.round_id
        OR r.currency <> NEW.currency
        OR r.external_transaction_id IS DISTINCT FROM NEW.reference_external_transaction_id
    THEN
        RAISE EXCEPTION 'transaction % does not match its reference %', NEW.id, NEW.reference_transaction_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NOT ((NEW.kind = 'WIN' AND r.kind = 'BET')
        OR (NEW.kind = 'REFUND' AND r.kind = 'BET')
        OR (NEW.kind = 'ROLLBACK' AND r.kind IN ('BET', 'WIN', 'REFUND')))
    THEN
        RAISE EXCEPTION '% cannot reference a %', NEW.kind, r.kind
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.kind IN ('REFUND', 'ROLLBACK') AND r.amount_minor <> NEW.amount_minor THEN
        RAISE EXCEPTION 'reversal % of % differs from the referenced amount %', NEW.id, NEW.amount_minor, r.amount_minor
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_reference_binding
    BEFORE INSERT OR UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_reference_binding();
