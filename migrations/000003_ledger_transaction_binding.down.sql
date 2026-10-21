DROP TRIGGER IF EXISTS inbox_messages_no_delete ON inbox_messages;
DROP TRIGGER IF EXISTS inbox_messages_guard ON inbox_messages;
DROP FUNCTION IF EXISTS inbox_messages_guard();

DROP TRIGGER IF EXISTS wager_transactions_processed_has_ledger ON wager_transactions;
DROP FUNCTION IF EXISTS wager_transactions_processed_has_ledger();

DROP TRIGGER IF EXISTS wallet_ledger_entries_chain ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_entries_chain();

DROP TRIGGER IF EXISTS wallet_ledger_entries_direction ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_entries_direction();

ALTER TABLE wallet_ledger_entries DROP CONSTRAINT IF EXISTS wallet_ledger_entries_transaction_binding_fk;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_ledger_binding_key;

-- Restore the guard of 000001.
CREATE OR REPLACE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status
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
