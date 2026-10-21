DROP TRIGGER IF EXISTS wager_transactions_reference_binding ON wager_transactions;
DROP FUNCTION IF EXISTS wager_transactions_reference_binding();
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_processed_reversal_resolved;

DROP TRIGGER IF EXISTS wager_transactions_currency ON wager_transactions;
DROP FUNCTION IF EXISTS wager_transactions_currency();
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_wallet_id_fk;
-- Fails if CURRENCY_MISMATCH rejections were recorded in the meantime: they
-- cannot exist under the stricter 000003 schema.
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_wallet_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency);

-- Chain function of 000003.
CREATE OR REPLACE FUNCTION wallet_ledger_entries_chain() RETURNS trigger LANGUAGE plpgsql AS $$
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
