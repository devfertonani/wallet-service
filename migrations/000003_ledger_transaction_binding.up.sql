-- Binds every ledger entry to the transaction it represents, so the database
-- rejects an entry that does not match its transaction even when the writer
-- bypasses the domain layer:
--   * same wallet, amount and currency as the transaction (composite FK);
--   * only PROCESSED transactions of a moving kind have entries, in the
--     direction of the kind (ROLLBACK: opposite of the referenced kind);
--   * the entry continues the wallet's ledger chain and the wallet ends the
--     transaction at the entry's version and balance;
--   * a PROCESSED moving transaction has its entry, and its result balance is
--     the balance after the entry.
-- It also closes two state regressions: PENDING_REFERENCE -> PENDING, and a
-- completed inbox message being reopened or rewritten.

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_ledger_binding_key UNIQUE (id, wallet_id, amount_minor, currency);

ALTER TABLE wallet_ledger_entries
    ADD CONSTRAINT wallet_ledger_entries_transaction_binding_fk
    FOREIGN KEY (transaction_id, wallet_id, amount_minor, currency)
    REFERENCES wager_transactions (id, wallet_id, amount_minor, currency);

CREATE FUNCTION wallet_ledger_entries_direction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    tx_kind text;
    tx_status text;
    tx_reference uuid;
    expected text;
BEGIN
    SELECT kind, status, reference_transaction_id INTO tx_kind, tx_status, tx_reference
    FROM wager_transactions WHERE id = NEW.transaction_id;
    IF tx_status IS DISTINCT FROM 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry for transaction % in status %', NEW.transaction_id, tx_status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    expected := CASE tx_kind
        WHEN 'OPENING' THEN 'CREDIT'
        WHEN 'WIN' THEN 'CREDIT'
        WHEN 'REFUND' THEN 'CREDIT'
        WHEN 'BET' THEN 'DEBIT'
        WHEN 'ROLLBACK' THEN (
            SELECT CASE kind WHEN 'BET' THEN 'CREDIT' WHEN 'WIN' THEN 'DEBIT' WHEN 'REFUND' THEN 'DEBIT' END
            FROM wager_transactions WHERE id = tx_reference)
    END;
    IF expected IS NULL OR NEW.direction <> expected THEN
        RAISE EXCEPTION 'ledger entry % has direction % for a % transaction', NEW.id, NEW.direction, tx_kind
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallet_ledger_entries_direction
    BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_direction();

CREATE FUNCTION wallet_ledger_entries_chain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    w_version bigint;
    w_balance bigint;
BEGIN
    IF NEW.wallet_version = 1 THEN
        IF NEW.balance_before <> 0 THEN
            RAISE EXCEPTION 'first ledger entry of wallet % does not start at zero', NEW.wallet_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries
        WHERE wallet_id = NEW.wallet_id AND wallet_version = NEW.wallet_version - 1
          AND balance_after = NEW.balance_before
    ) THEN
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

CREATE CONSTRAINT TRIGGER wallet_ledger_entries_chain
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_chain();

CREATE FUNCTION wager_transactions_processed_has_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'PROCESSED' AND NEW.kind <> 'LOSS' AND NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries
        WHERE wallet_id = NEW.wallet_id AND transaction_id = NEW.id
          AND balance_after = NEW.result_balance_minor
    ) THEN
        RAISE EXCEPTION 'processed % transaction % has no matching ledger entry', NEW.kind, NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wager_transactions_processed_has_ledger
    AFTER INSERT OR UPDATE ON wager_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_processed_has_ledger();

-- Same guard as 000001, plus the allowed transitions and a monotonic retry counter.
CREATE OR REPLACE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
        RAISE EXCEPTION 'wager transaction % cannot go back from PENDING_REFERENCE to PENDING', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.attempts < OLD.attempts THEN
        RAISE EXCEPTION 'attempts of wager transaction % cannot decrease', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.origin IS DISTINCT FROM OLD.origin
        OR NEW.kind IS DISTINCT FROM OLD.kind
        OR NEW.wallet_id IS DISTINCT FROM OLD.wallet_id
        OR NEW.player_id IS DISTINCT FROM OLD.player_id
        OR NEW.amount_minor IS DISTINCT FROM OLD.amount_minor
        OR NEW.currency IS DISTINCT FROM OLD.currency
        OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
        OR NEW.round_id IS DISTINCT FROM OLD.round_id
        OR NEW.game_id IS DISTINCT FROM OLD.game_id
        OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'immutable fields of wager transaction % cannot change', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION inbox_messages_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.completed_at IS NOT NULL
        OR NEW.consumer_name IS DISTINCT FROM OLD.consumer_name
        OR NEW.message_id IS DISTINCT FROM OLD.message_id
        OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
        OR NEW.received_at IS DISTINCT FROM OLD.received_at
    THEN
        RAISE EXCEPTION 'inbox message %/% is immutable', OLD.consumer_name, OLD.message_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER inbox_messages_guard
    BEFORE UPDATE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION inbox_messages_guard();

CREATE TRIGGER inbox_messages_no_delete
    BEFORE DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION forbid_change();
