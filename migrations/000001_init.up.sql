-- Financial invariants live here, not only in Go: uniqueness, non negative
-- balances, ledger arithmetic and ledger immutability hold even if a buggy or
-- concurrent writer bypasses the domain layer.

CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint      NOT NULL CHECK (balance_minor >= 0),
    version       bigint      NOT NULL CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

-- Used by the composite foreign keys below, so a transaction or a ledger entry
-- can never point at a wallet with another currency.
ALTER TABLE wallets ADD CONSTRAINT wallets_id_currency_key UNIQUE (id, currency);

CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         uuid        NOT NULL,
    player_id                         uuid        NOT NULL,
    amount_minor                      bigint      NOT NULL CHECK (amount_minor >= 0),
    currency                          char(3)     NOT NULL,

    -- external origin only
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    round_id                          text,
    game_id                           text,
    reference_external_transaction_id text,

    -- resolution and result
    reference_transaction_id          uuid REFERENCES wager_transactions (id),
    failure_code                      text,
    result_balance_minor              bigint CHECK (result_balance_minor >= 0),
    correlation_id                    text,

    -- retry bookkeeping for PENDING_REFERENCE
    attempts                          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   timestamptz,
    expires_at                        timestamptz,

    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,

    CONSTRAINT wager_transactions_wallet_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),

    -- OPENING is the only internal kind, and internal rows carry no provider metadata.
    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),

    -- Zero policy: LOSS is always 0.00; every other kind moves a positive amount.
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),

    CONSTRAINT wager_transactions_reversal_needs_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),

    CONSTRAINT wager_transactions_status_shape CHECK (
        (status = 'PROCESSED' AND result_balance_minor IS NOT NULL AND failure_code IS NULL)
        OR (status IN ('REJECTED', 'FAILED') AND failure_code IS NOT NULL)
        OR (status IN ('PENDING', 'PENDING_REFERENCE') AND failure_code IS NULL)
    ),

    CONSTRAINT wager_transactions_pending_reference_schedule CHECK (
        status <> 'PENDING_REFERENCE' OR (next_attempt_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);

-- One financial operation per (provider, external id), whatever the key used.
CREATE UNIQUE INDEX wager_transactions_provider_external_key
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';

-- One operation per idempotency key, scoped to the provider that owns it.
CREATE UNIQUE INDEX wager_transactions_provider_idempotency_key
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';

-- A wallet is opened at most once with a positive balance.
CREATE UNIQUE INDEX wager_transactions_one_opening_per_wallet
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- A referenced transaction is compensated at most once with success, by a
-- REFUND or by a ROLLBACK, never both. ARCHITECTURE.md explains the policy.
CREATE UNIQUE INDEX wager_transactions_one_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

-- Pending-reference worker scan.
CREATE INDEX wager_transactions_pending_reference_due
    ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';

-- Lookup of reversals waiting for a given reference.
CREATE INDEX wager_transactions_waiting_reference
    ON wager_transactions (provider_id, reference_external_transaction_id) WHERE status = 'PENDING_REFERENCE';

CREATE INDEX wager_transactions_wallet ON wager_transactions (wallet_id, created_at);

-- Terminal rows are frozen, and the identity and financial fields of any row
-- never change after insert.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
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

CREATE TRIGGER wager_transactions_guard
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

CREATE FUNCTION forbid_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is forbidden: the table is append-only', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER wager_transactions_no_delete
    BEFORE DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_change();

CREATE TABLE wallet_ledger_entries (
    seq            bigint      GENERATED ALWAYS AS IDENTITY UNIQUE,
    id             uuid        PRIMARY KEY,
    wallet_id      uuid        NOT NULL,
    transaction_id uuid        NOT NULL REFERENCES wager_transactions (id),
    direction      text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),
    currency       char(3)     NOT NULL,
    balance_before bigint      NOT NULL CHECK (balance_before >= 0),
    balance_after  bigint      NOT NULL CHECK (balance_after >= 0),
    wallet_version bigint      NOT NULL CHECK (wallet_version >= 1),
    created_at     timestamptz NOT NULL,

    CONSTRAINT wallet_ledger_entries_wallet_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
    CONSTRAINT wallet_ledger_entries_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after = balance_before + amount_minor)
        OR (direction = 'DEBIT' AND balance_after = balance_before - amount_minor)
    )
);

CREATE INDEX wallet_ledger_entries_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

CREATE TRIGGER wallet_ledger_entries_no_update
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_change();

CREATE TRIGGER wallet_ledger_entries_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();

-- A balance change is only valid together with the ledger entry that explains
-- it, in the same transaction. Deferred so the wallet row and the entry can be
-- written in any order before commit.
CREATE FUNCTION wallets_balance_has_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.balance_minor IS DISTINCT FROM OLD.balance_minor THEN
        IF NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'wallet % balance changed without a version increment', NEW.id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM wallet_ledger_entries
            WHERE wallet_id = NEW.id
              AND wallet_version = NEW.version
              AND balance_before = OLD.balance_minor
              AND balance_after = NEW.balance_minor
        ) THEN
            RAISE EXCEPTION 'wallet % balance changed without a matching ledger entry', NEW.id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF NEW.version IS DISTINCT FROM OLD.version THEN
        RAISE EXCEPTION 'wallet % version changed without a balance change', NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.player_id IS DISTINCT FROM OLD.player_id
        OR NEW.currency IS DISTINCT FROM OLD.currency OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'immutable fields of wallet % cannot change', NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_balance_has_ledger
    AFTER UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_balance_has_ledger();

CREATE FUNCTION wallets_opening_has_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.version <> 1 THEN
        RAISE EXCEPTION 'wallet % must be created with version 1', NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor > 0 AND NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries
        WHERE wallet_id = NEW.id AND wallet_version = 1
          AND direction = 'CREDIT' AND balance_before = 0 AND balance_after = NEW.balance_minor
    ) THEN
        RAISE EXCEPTION 'wallet % opened with a balance but without its OPENING ledger entry', NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_opening_has_ledger
    AFTER INSERT ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_opening_has_ledger();

CREATE TRIGGER wallets_no_delete
    BEFORE DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION forbid_change();

CREATE TABLE inbox_messages (
    consumer_name  text        NOT NULL,
    message_id     text        NOT NULL,
    payload_hash   text        NOT NULL,
    transaction_id uuid        REFERENCES wager_transactions (id),
    received_at    timestamptz NOT NULL,
    completed_at   timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    seq             bigint      GENERATED ALWAYS AS IDENTITY UNIQUE,
    id              uuid        PRIMARY KEY,
    aggregate_type  text        NOT NULL,
    aggregate_id    uuid        NOT NULL,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL CHECK (event_version >= 1),
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL,
    locked_by       text,
    locked_until    timestamptz,
    published_at    timestamptz,
    last_error      text
);

CREATE INDEX outbox_events_due ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;

-- The payload is an immutable snapshot; only delivery bookkeeping may change,
-- and a published event is never reopened.
CREATE FUNCTION outbox_events_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.aggregate_type IS DISTINCT FROM OLD.aggregate_type
        OR NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id OR NEW.event_type IS DISTINCT FROM OLD.event_type
        OR NEW.event_version IS DISTINCT FROM OLD.event_version OR NEW.payload IS DISTINCT FROM OLD.payload
        OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
        OR (OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at)
    THEN
        RAISE EXCEPTION 'outbox event % is immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_guard
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();

CREATE TRIGGER outbox_events_no_delete
    BEFORE DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION forbid_change();
