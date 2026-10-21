# Architecture

This document records the technical decisions of the wallet service, the reason behind each one, and what was left out.
The [Implementation status](#implementation-status) section says what is already in the code and what is not yet.
[Running from a clean checkout](#running-from-a-clean-checkout) and [Testing](#testing) say how to start the environment and run every test tier.
[Eliminatory criteria](#eliminatory-criteria) maps each eliminatory criterion of the challenge to its mechanism and its proof.

## Overview

A single binary (`cmd/wallet`) runs the HTTP API, the SQS consumer, the pending-reference worker and the outbox publisher.
Several instances run at the same time against the same PostgreSQL and the same SQS.
No instance holds state that another one needs: all coordination happens in the database.
[Multiple instances](#multiple-instances) describes how the instances are run and what is tested against them, and [Failure matrix](#failure-matrix) explains why the money stays correct when they fail between steps.

```
            HTTP (JWT)                       SQS FIFO
                │                       wager-transactions.fifo
                ▼                                │
        ┌───────────────┐                ┌───────────────┐
        │ http handlers │                │ sqs consumer  │──► inbox
        └───────┬───────┘                └───────┬───────┘
                └──────────────┬─────────────────┘
                               ▼
                  ┌─────────────────────────┐
                  │ use cases (app)         │  one SQL transaction per operation
                  │ pure domain (domain)    │
                  └────────────┬────────────┘
                               ▼
                         PostgreSQL ◄──── pending-reference worker
                    (balance, ledger, inbox,
                     outbox, constraints)  ◄──── outbox publisher ──► SNS FIFO
                                                                    wallet-events.fifo
```

The database is the source of truth for the financial invariants.
The Go code validates the same rules first, to return a readable business error, but correctness does not depend on it: a buggy or concurrent writer runs into the constraints.

**Toolchain.**
Go 1.26.10, declared in `go.mod` (`go 1.26.10`, no `toolchain` directive).
The build stages of both Dockerfiles use the same version: `Dockerfile` (`golang:1.26.10-alpine`, with `GOTOOLCHAIN=local`) and `loadtest/Dockerfile.loadgen`.
The runtime image is `gcr.io/distroless/static-debian12:nonroot`, with no Go in it.

## Package layout

| Package | Responsibility | Imports (checked with `go list`) |
| --- | --- | --- |
| `internal/domain/money`, `wallet`, `ledger`, `wager`, `events`, `failure` | `Money`, wallet, ledger entry, transaction with its state machine and per-kind rules, events, failure codes | standard library, `uuid`, other domain packages |
| `internal/app/wagering` | use cases and ports | domain, `uuid`, standard library |
| `internal/adapters/pgstore` | pgx unit of work and repositories, classification of database errors | app, domain, pgx |
| `internal/adapters/httpapi` | HTTP handlers, per-route authentication/authorization, error mapping | app, domain, `config`, `platform/auth`, `platform/httpserver`, `platform/metrics`, Fx |
| `internal/adapters/sqsconsumer` | SQS consumer, inbox use, retry, DLQ | app, domain, `config`, `platform/awsclient`, `platform/metrics`, `platform/worker`, AWS SDK (SQS, SNS), Fx |
| `internal/adapters/outbox` | outbox publisher to SNS | `config`, `platform/awsclient`, `platform/metrics`, `platform/worker`, pgx, AWS SDK (SNS), Fx; no app or domain |
| `internal/adapters/pending` | pending-reference worker | app, `config`, `platform/worker`, Fx |
| `internal/config` | reads and validates the environment | Fx |
| `internal/health` | liveness and readiness | pgx pool, AWS SDK (SQS), `platform/awsclient`, `platform/httpserver`, Fx |
| `internal/platform/...` | pool and migrator, AWS clients, HTTP server, logs, metrics, JWT validation, worker lifecycle | I/O libraries, Fx |
| `internal/testsupport/...` | `pgtest` (throwaway database), `awstest` (throwaway queues and topic), `dotenv` | test helpers only |
| `internal/app/app.go` | Fx composition | everything |

The domain knows nothing about Fx, HTTP, SQS or pgx, and neither does `internal/app/wagering`: no non-test file under either imports `go.uber.org/fx`, `net/http`, the AWS SDK or pgx.
Fx appears in the `Module` declaration of every adapter, platform package, `config` and `health`, and in the composition.

**Ports.**
`internal/app/wagering/ports.go` declares the ports the use cases depend on: `WalletRepository`, `TransactionRepository`, `LedgerRepository`, `OutboxRepository`, `InboxRepository`, `UnitOfWork` and `Recorder` (business metrics).
Adapters are bound to them in two places: compile-time assertions such as `var _ wagering.UnitOfWork = (*UnitOfWork)(nil)` in `pgstore`, and the providers of `wageringModule` in `internal/app/app.go` (`pgstore.NewUnitOfWork` as `wagering.UnitOfWork`, `*metrics.Metrics` as `wagering.Recorder`).

**Enforcement.**
The import boundary is a convention checked in review; no linter (`depguard`) or test enforces it.

## Money

`Money` is an immutable value object with an `int64` in minor units (cents) and a `Currency`.
No `float32` or `float64` takes part in parsing, arithmetic, serialization or persistence.
The only floats in the service are Prometheus values (metrics, outbox lag in seconds).

**Why `int64` and not a decimal library.**
The scale is fixed at two decimal places, so integer cents represent every value without loss.
Addition and comparison become integer operations, persistence becomes `BIGINT`, and there is no external dependency to audit.
A decimal library would solve variable scale, which the challenge does not ask for.

**Limits and overflow.**
The range is that of `int64`: from -92,233,720,368,547,758.08 to 92,233,720,368,547,758.07.
Parsing accumulates the digits on the negative side (`acc = acc*10 - d`) and checks `acc < (MinInt64+d)/10` before each step, so `"-92233720368547758.08"` parses to `math.MinInt64`.
Its positive counterpart `"92233720368547758.08"` is rejected with `money.ErrOverflow`, because `-MinInt64` is not representable.
`Add` and `Sub` check the bounds before the operation (`m > MaxInt64-o`, `m < MinInt64-o` and the symmetric cases), so the overflowing operation never runs.
`Neg` has one overflowing input, `math.MinInt64`, and returns `money.ErrOverflow` for it.
`Amount()` formats `MinInt64` safely through `uint64`.
The ledger sum used by reconciliation runs in SQL as `numeric` and is converted back to `int64` only if it fits; otherwise it returns `money.ErrOverflow`.
A parse overflow surfaces as `400 INVALID_AMOUNT`; a credit that would overflow the balance is the stored rejection `BALANCE_LIMIT_EXCEEDED`.
`TestParseOverflow` and `TestArithmeticOverflow` cover each case, `Neg(MinInt64)` included.

**Accepted format.**
Input is accepted only in canonical form: optional sign, integer part without leading zeros, a dot and exactly two decimal places (`"25.00"`, `"0.50"`).
The following are rejected, with no rounding or normalization: `""`, `"25"`, `"25.5"`, `"25.005"`, `"025.00"`, `"+25.00"`, `"2.5e1"`, `"NaN"`, `"Infinity"`, spaces, comma and `"-0.00"`.
A numeric JSON value (`{"amount": 25.00}`) is also rejected, because it would go through `float64` in the decoder.
Since only the canonical form gets in, the received string is the same one that goes into the idempotency hash; there is no amount normalization step.

**Serialization.**
`Money` implements `MarshalJSON` and `UnmarshalJSON` through `money.DTO {amount string, currency string}`.
Output is built with integer division and modulo only (`sign + minor/100 + "." + two-digit minor%100`).
Every amount field of every DTO is a Go `string`, so `encoding/json` returns an `UnmarshalTypeError` on a JSON number before any `float64` exists; the HTTP decoder maps it to `400 INVALID_AMOUNT`, and the SQS envelope decoder treats it as an invalid message.
`TestJSONContract` and `TestJSONRejectsNumbersAndBadStrings` cover this.

**Currency.**
Upper-case ISO 4217 code, among the supported two-decimal currencies (`BRL`, `USD`, `EUR`, `GBP`, `MXN`, `ARS`); `ParseCurrency` is case sensitive.
Currencies with a different minor unit (`JPY`, `CLP`, `KWD`) are left out on purpose, because they would break the fixed scale.
`Add`, `Sub` and `Cmp` require the same currency and return `money.ErrCurrencyMismatch` otherwise; the result keeps the receiver's currency.
`Equal` returns no error: values in different currencies are simply not equal.
The main scenarios run in BRL; the tests cover currency mismatch.

The rule is enforced in layers, and each one runs before any arithmetic:

1. the `wager` rules compare the operation's currency with the wallet's right after the player check and record a `CURRENCY_MISMATCH` rejection, with no balance revealed; a reference in another currency is `REFERENCE_MISMATCH`;
2. `Wallet.Apply` refuses an amount in another currency with `money.ErrCurrencyMismatch`;
3. `ledger.New` recomputes `before ± amount` with `Add`/`Sub` and checks the result with `Equal`;
4. the database is the last line: `CHECK (currency ~ '^[A-Z]{3}$')`, the composite foreign keys `(wallet_id, currency)` on the ledger and `(transaction_id, wallet_id, amount_minor, currency)`, the currency trigger on transactions, and the reference-binding trigger.

**Uninitialized zero value.**
`Money{}` has no currency.
`Add`, `Sub`, `Neg`, `Cmp`, `Zero`, `FromMinor` and `MarshalJSON` reject it with `money.ErrUninitialized`; `IsValid()` exposes the check.
`Amount()`, `String()`, `DTO()`, `Equal()` and the sign predicates do not check it, so `Money{}.DTO()` would render `{"amount":"0.00","currency":""}`.
The protection is that no constructor or rehydration function lets a `Money{}` into an entity (see [Domain model](#domain-model)), so serialized money always comes from a valid value.

**External input.**
`money.ParseNonNegative` is the parser for values coming from providers and the API; it rejects negatives.
Negative values exist only in internal calculations (the reconciliation `difference`, for example).

**Persistence.**
The stored value is exactly the domain value, with no conversion; no `float` or `NUMERIC` column stores money.

| Table | Money columns | Checks |
| --- | --- | --- |
| `wallets` | `balance_minor BIGINT`, `currency CHAR(3)` | `balance_minor >= 0`, currency format |
| `wager_transactions` | `amount_minor BIGINT`, `currency CHAR(3)`, `result_balance_minor BIGINT` (nullable) | `amount_minor >= 0` plus the per-kind zero policy, `result_balance_minor >= 0` |
| `wallet_ledger_entries` | `amount_minor BIGINT`, `currency CHAR(3)`, `balance_before BIGINT`, `balance_after BIGINT` | `amount_minor > 0`, both balances `>= 0`, `balance_after = balance_before ± amount_minor` |
| `outbox_events` | inside the `jsonb` payload, as the `{amount, currency}` string DTO | none: it is an immutable snapshot |

On read, `pgstore` calls `money.ParseCurrency` and then `money.FromMinor(minor, currency)`; on write, it calls `Minor()` and `Currency().String()`.
`ResultBalance` maps to `*int64`, which is `NULL` when there is no result balance.

## Persistence and transactions

**Library.**
`pgx/v5` with `pgxpool` and explicit SQL, with no ORM and no code generator.
Locks, isolation levels, constraint names and transaction boundaries stay visible in the code, which is what needs to be audited in a financial system.

**Transaction boundary.**
The use case opens the transaction through the `wagering.UnitOfWork` port and receives the repositories already bound to it:

```go
err := s.uow.Run(ctx, func(ctx context.Context, r Repos) error {
    w, err := r.Wallets.GetForUpdate(ctx, walletID)
    ...
    return r.Outbox.Append(ctx, events...)
})
```

`UnitOfWork` has two methods.
`Run` runs `fn` in `READ COMMITTED` and commits if `fn` returns nil; it is used by `Submit`, the FAILED audit record, the pending-reference worker and `OpenWallet`.
`Read` runs `fn` in `REPEATABLE READ READ ONLY`, so every query sees the same snapshot; every read use case uses it (`GetWallet`, `LedgerPage`, `GetTransaction`, `GetByExternalID`, `Reconcile`, and the listing of due pending operations).
The pgx adapter (`pgstore.UnitOfWork`) calls `BeginTx`, defers a `Rollback` on a context detached from cancellation (a no-op after the commit), builds every repository as a struct wrapping the same `pgx.Tx`, runs `fn` and then commits.
Repositories never open or commit a transaction.
Everything an operation changes (wager transaction, wallet, ledger entry, inbox, outbox) is confirmed in a single commit, or nothing is.
The deferred constraint triggers run at `COMMIT`, so commit errors go through the same classification as statement errors.
The outbox publisher deliberately runs outside the unit of work: its claim, confirmation and reschedule are single autocommit statements on the pool.

**Error classification.**
`pgstore` classifies every error by SQLSTATE and, for unique violations, by constraint name:

| Error | Classified as |
| --- | --- |
| `23505` on `wallets_player_currency_key` | `ErrWalletExists` (`409`) |
| `23505` on `wager_transactions_provider_external_key`, `wager_transactions_provider_idempotency_key` or `inbox_messages_pkey` | `ErrDuplicate` (a concurrent copy; retried) |
| any other `23xxx`, trigger errors included (`23000`) | `ErrIntegrity` (recorded as `FAILED`) |
| `55P03` (`lock_timeout`), `40P01` (deadlock) | `ErrTransient` plus `ErrLockConflict` |
| `40001`, `57014`, `57P01`–`57P03`, `53300`, class `08` | `ErrTransient` |
| connect, network and timeout errors, `pgconn.SafeToRetry`, context deadline or cancel, `EOF`, closed transaction, connection or pool | `ErrTransient` |

**Migrations.**
Versioned files in `migrations/`, named `NNNNNN_name.up.sql`/`.down.sql`: `000001_init`, `000002_app_role_grants`, `000003_ledger_transaction_binding` and `000004_reversal_binding_and_zero_opening`.
They are applied with `golang-migrate` v4.20.1 (pgx/v5 driver, `iofs` source) and embedded in the binary with `//go:embed *.sql`, so that the `migrate` command, the tests and the environment use the same files.
`cmd/migrate` reads `MIGRATIONS_DATABASE_URL`, which must be the schema owner (`wallet_owner`); `postgres://` URLs are rewritten to the driver's `pgx5://` scheme.
`migrate up` applies and prints `ok, version N` (or `no change`), `migrate down N` reverts N steps (N is mandatory, so that nobody reverts everything by mistake) and `migrate version` prints `version N dirty=false` or `no migration applied`.
The `down` of the first migration deletes every financial record and only makes sense in a local environment.
The service never migrates on start; it connects as `wallet_service`.

How migrations run:

- **Compose:** the one-shot `migrate` service runs `/app/migrate up` as `wallet_owner` once Postgres is healthy, and the wallet instances wait for it with `service_completed_successfully`.
  By hand: `docker compose run --rm --entrypoint /app/migrate migrate version` (or `up`, `down 1`), or `MIGRATIONS_DATABASE_URL=... go run ./cmd/migrate up` from the host (`.env.example` has the host URL, on port 55432).
- **Tests:** `pgtest` creates a throwaway database per test (`wallet_test_<uuid>`, owned by `wallet_owner`) and applies the embedded migrations with `postgres.MigrateUp`; `TestMigrationsRevertAndReapply` runs every `down`, checks that nothing is applied, runs every `up` again and checks version 4, not dirty.

**Database roles.**
`deploy/postgres/init.sql` creates the roles and the `wallet` database when the Postgres volume is first initialized.
The service connects as `wallet_service`, a member of `wallet_app`, and does not own the schema.
`wallet_owner` is the owner and is used only by the migration command.
`wallet_app` has `SELECT` and `INSERT` on the five tables and `UPDATE` only on `wallets`, `wager_transactions`, `inbox_messages` and `outbox_events`: no `UPDATE`, `DELETE` or `TRUNCATE` on the ledger, and no `DELETE` on any table.

## Invariants enforced by the database

| Invariant | Mechanism |
| --- | --- |
| One wallet per `(playerId, currency)` | `UNIQUE (player_id, currency)` (`wallets_player_currency_key`) |
| Balance never negative | `CHECK (balance_minor >= 0)` |
| Balance changes only with a matching ledger entry | constraint trigger deferred to commit (`wallets_balance_has_ledger`): a balance change requires version +1 and a ledger entry whose `balance_before` and `balance_after` equal the old and the new balance |
| Version changes only with a balance change | the same trigger |
| A wallet opened with a balance has the opening ledger entry | deferred constraint trigger on `INSERT` (`wallets_opening_has_ledger`) |
| Consistent ledger entry | `CHECK (balance_after = balance_before ± amount)` according to the direction (`wallet_ledger_entries_arithmetic`) |
| One ledger entry per `(walletId, transactionId)` and per `(walletId, walletVersion)` | `UNIQUE` (`wallet_ledger_entries_wallet_transaction_key`, `wallet_ledger_entries_wallet_version_key`) |
| Immutable ledger | no write privilege for `wallet_app` and a trigger that rejects `UPDATE`, `DELETE` and `TRUNCATE` even for the owner |
| Ledger entry currency equals the wallet's | composite foreign key `(wallet_id, currency)` (`wallet_ledger_entries_wallet_fk`) |
| Transaction currency equals the wallet's, except for the `CURRENCY_MISMATCH` rejection itself | trigger on `INSERT`/`UPDATE` (000004; before it was the composite FK, which prevented storing that rejection) |
| One operation per `(providerId, externalTransactionId)` | partial unique index `wager_transactions_provider_external_key`, `WHERE origin = 'EXTERNAL'` |
| One operation per `(providerId, idempotencyKey)` | partial unique index `wager_transactions_provider_idempotency_key`, `WHERE origin = 'EXTERNAL'` |
| Internal and external operations have different shapes | `CHECK wager_transactions_origin_shape`, see [OPENING](#opening) |
| `OPENING` at most once per wallet | partial unique index `wager_transactions_one_opening_per_wallet` |
| Zero-amount policy per kind | `CHECK wager_transactions_amount_policy`: `LOSS` requires zero, the others require a positive amount |
| One successful compensation per reference | partial unique index `wager_transactions_one_reversal_per_reference` on `reference_transaction_id` for processed `REFUND`/`ROLLBACK` |
| A terminal transaction does not change | trigger on `UPDATE` |
| Identity and amount of a transaction do not change | the same trigger |
| Status and its fields agree | `CHECK wager_transactions_status_shape` and `wager_transactions_pending_reference_schedule` |
| Immutable outbox payload; a published event does not reopen; outbox rows are not deleted | trigger on `UPDATE` and trigger on `DELETE` |
| One inbox entry per `(consumerName, messageId)` | primary key `inbox_messages_pkey` |
| A ledger entry belongs to the transaction it represents: same wallet, amount and currency | composite foreign key `(transaction_id, wallet_id, amount_minor, currency)` (000003) |
| Only a `PROCESSED` transaction of a moving kind has a ledger entry, in the kind's direction | trigger on ledger `INSERT`; `ROLLBACK` requires the direction opposite to the reference's (000003) |
| Chained ledger: `balance_before` of entry n is the `balance_after` of entry n−1, and the wallet ends the commit at the version and balance of the last entry | deferred constraint trigger `wallet_ledger_entries_chain` (000003) |
| A `PROCESSED` transaction of a moving kind has its ledger entry, with `balance_after` equal to the balance returned to the provider | deferred constraint trigger `wager_transactions_processed_has_ledger` (000003) |
| No `PENDING_REFERENCE → PENDING` regression; `attempts` never decreases | trigger on `UPDATE` (000003) |
| A completed inbox message does not change and is not deleted | trigger on `UPDATE` and `DELETE` (000003) |
| A processed `REFUND`/`ROLLBACK` points to its reference; the reference is `PROCESSED`, in the same wallet, player, provider, round and currency, is of an allowed kind and, for reversals, has the same amount | `CHECK wager_transactions_processed_reversal_resolved` and trigger `wager_transactions_reference_binding` (000004) |
| The first ledger entry of a wallet starts at zero: version 1 (opened with a balance) or 2 (opened with zero) | deferred constraint trigger (000004) |

Constraints and indexes are named explicitly, and the code depends on those names: the unique-violation classification in `pgstore` maps each known name to a domain error.
The four constraint triggers are `DEFERRABLE INITIALLY DEFERRED`, so they fire at `COMMIT` and see the whole operation; their errors raise `integrity_constraint_violation`, classified as `ErrIntegrity`.
The integration tests in `migrations/schema_integration_test.go` try to break every row of this table, including as the schema owner.
Migration 000004 came out of the second review: 000003 refused the first credit of a wallet opened with zero, the currency FK prevented storing the `CURRENCY_MISMATCH` rejection (the operation became a `503` forever), and the database accepted two `PROCESSED` reversals with no reference or with an amount different from the referenced one.
Migration 000003 came out of a review that managed, with direct SQL as `wallet_service`, to store a 1.00 `BET` producing a 999.99 credit, a ledger entry for a `REJECTED` transaction and a ledger entry with no balance change; `TestLedgerIsBoundToItsTransaction` reproduces each of these attacks and shows the database refusing them.

## Domain model

**Encapsulated state.**
Every entity keeps its state in unexported fields reachable only through getters, with no setters: `wallet.Wallet`, `ledger.Entry`, `wager.Command`, `money.Money` and `money.Currency`.
`wager.Transaction` wraps an unexported `Snapshot`; `Snapshot()` returns a copy, with `ResultBalance` deep-copied, so changing it does not change the transaction.
State changes only through explicit methods: `Wallet.Apply` for the balance and the transition methods of `wager.Transaction` for the status.

**Creation and rehydration.**
Creation and rehydration are separate functions:

| Entity | Creation | Rehydration |
| --- | --- | --- |
| Wallet | `wallet.Open` | `wallet.Rehydrate` |
| Wager transaction | `wager.NewExternal` (from a validated `wager.NewCommand`), `wager.NewOpening` | `wager.Rehydrate(Snapshot)` |
| Ledger entry | `ledger.New` | `ledger.New` |

Creation validates the input and applies the creation rule: `wallet.Open` creates version 1 and, with a positive balance, the opening credit; `NewExternal` starts in `PENDING`; `NewOpening` is processed at once.
Rehydration validates the consistency of the stored state, but replays no transition, does not reapply movements, does not change the version and does not emit events; events are built only by the `events.New*` constructors inside a use case.
A ledger entry has a single function for both paths, because it is immutable and neither path changes any state, so both share the same validation.
The `pgstore` repositories call `wallet.Rehydrate`, `wager.Rehydrate` and `ledger.New`; a stored row that fails validation is reported as an error (`ErrInvalidWallet "inconsistent stored state"`, `ErrInvalidTransaction`, `ErrInvalidEntry`), never silently fixed.

**Uninitialized and invalid values.**
Since fields are unexported, a valid value can only come from a validating constructor or a rehydration function, and each of them rejects the zero values:

- `money`: `ParseCurrency` accepts only the six supported codes; `Zero(Currency{})` and `FromMinor(_, Currency{})` return `ErrUninitialized`;
- `wallet.Open` rejects a nil id or player, an uninitialized `Money` (`ErrInvalidWallet` wrapping `money.ErrUninitialized`) and a negative balance; `wallet.Rehydrate` also rejects a version below 1; `Apply` requires a positive amount;
- `ledger.New` rejects nil ids, an invalid direction, any uninitialized amount or balance, a non-positive amount, negative balances, arithmetic that does not hold, a version below 1 and a zero `createdAt`;
- `wager.NewCommand` rejects empty or oversized identifiers, nil or malformed UUIDs, unknown or internal kinds, invalid money and reference errors, each as a correctable `*failure.Error`;
- `wager.NewExternal` rejects a zero-value `Command{}` (empty payload hash), and `wager.Rehydrate` rejects nil ids, invalid money, unknown kind or status and every shape mismatch.

The database checks are the backstop (`CHECK (balance_minor >= 0)`, the per-kind amount policy, the shape checks).

**Errors.**
Domain errors are classifiable without string matching:

- sentinels checked with `errors.Is`: `money.ErrInvalidAmount`, `ErrInvalidCurrency`, `ErrNegativeAmount`, `ErrOverflow`, `ErrCurrencyMismatch`, `ErrUninitialized`; `wallet.ErrInvalidWallet`, `ErrInsufficientFunds`; `ledger.ErrInvalidEntry`; `wager.ErrInvalidTransaction`, `ErrInvalidTransition`;
- the typed error `*failure.Error{Code failure.Code; Message string}` in `internal/domain/failure`, extracted with `errors.As`, `failure.CodeOf(err)` or `failure.Is(err, code)`; its `Is` method matches another `*failure.Error` with the same code;
- the application sentinels in `internal/app/wagering/ports.go`: `ErrNotFound`, `ErrWalletExists`, `ErrDuplicate`, `ErrConcurrentUpdate`, `ErrTransient`, `ErrIntegrity`, `ErrLockConflict`, `ErrIdempotencyKeyReused`, `ErrDuplicateExternal`, `ErrMessageIDReused`.

Failure codes come in three classes: correctable (`INVALID_REQUEST`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `UNSUPPORTED_KIND`, `MISSING_REFERENCE`, `WALLET_NOT_FOUND`), definitive business rejections (`Code.Definitive()`), and `INVARIANT_VIOLATION`.
A definitive rejection is not a Go error at all: `wager.Settle` translates the domain sentinels (`ErrInsufficientFunds` into `INSUFFICIENT_FUNDS` or, for a `ROLLBACK`, `REVERSAL_INSUFFICIENT_FUNDS`; `ErrOverflow` into `BALANCE_LIMIT_EXCEEDED`) and returns a result whose transaction is `REJECTED`; `Transaction.Reject` refuses non-definitive codes.
`panic` does not represent a business rejection; the only panics are `money.MustCurrency` (constants in tests and wiring) and a JSON encoding failure in `CanonicalJSON`, which would be a bug, and a worker loop recovers from any panic.
[HTTP contract](#http-contract) and [SQS consumer](#sqs-consumer) give the mapping of each error to a status or a queue action, and [Context, cancellation and deadlines](#context-cancellation-and-deadlines) the I/O policy.

**Wallet.**
The wallet is the aggregate root, with the fields `id`, `playerID`, `balance`, `version`, `createdAt` and `updatedAt`; the currency is not a separate field, it is the balance's currency.
`wallet.Open(id, playerID, initial, openingTxID, entryID, now)` returns the wallet at version 1 and, with a positive balance, the opening `ledger.Entry` (credit from zero to the initial balance, linked to `openingTxID`, wallet version 1); with a zero balance there is no entry.
`wallet.Rehydrate(id, playerID, balance, version, createdAt, updatedAt)` rebuilds a stored wallet.
`Wallet.Apply(dir ledger.Direction, amount, transactionID, entryID, now)` is the single movement method, for both `ledger.Debit` and `ledger.Credit`.
It has a value receiver, so the receiver does not change; it returns the next wallet (new balance, version + 1, `updatedAt`) and the `ledger.Entry`.
It refuses a non-positive amount, an amount in another currency (`money.ErrCurrencyMismatch`), a debit below zero (`ErrInsufficientFunds`) and an unknown direction.
The version starts at 1 and only goes up when the balance changes; `LOSS` does not change the version.

**Ledger entry (`ledger.Entry`).**
Fields: `id`, `walletID`, `transactionID`, `direction` (`DEBIT`/`CREDIT`), `amount`, `balanceBefore`, `balanceAfter`, `walletVersion` and `createdAt`; the table adds `seq`, an identity column used for ordering and pagination.
`ledger.New` validates non-nil ids, a valid direction, initialized and single-currency amounts, `amount > 0`, non-negative balances, `balanceAfter = balanceBefore ± amount` by direction, `walletVersion >= 1` and a non-zero timestamp, stored in UTC.
The column checks mirror the constructor one for one: `amount_minor > 0`, `balance_before >= 0`, `balance_after >= 0`, `wallet_version >= 1`, and `CHECK wallet_ledger_entries_arithmetic` for the arithmetic.
`LOSS` and rejected operations produce no ledger entry.
Double-entry bookkeeping was not implemented; the ledger is single-entry per wallet.

**Append-only ledger.**
Ledger entries are never updated or deleted: `wallet_app` has only `SELECT` and `INSERT` on the table, and a trigger rejects `UPDATE`, `DELETE` and `TRUNCATE` even for the owner; the code only ever inserts.
A correction is a new transaction with its own entry in the opposite direction: a `REFUND` credits back a bet, and a `ROLLBACK` goes against the referenced kind (`Direction.Opposite()`).
The original entry stays, so the history plus the reversal rebuilds the balance, and reconciliation sums the whole ledger.
The wallet row is the only mutable projection, and it can change only together with a new ledger entry (`wallets_balance_has_ledger`).
`wager_transactions` is not fully append-only: its status fields change through the state machine, its identity fields are frozen by a trigger, and `DELETE` is forbidden.
`TestLedgerIsAppendOnlyAndConsistent` checks the privilege error for the application role and the trigger error for the owner.

**WagerTransaction.**

| Field (`wager.Snapshot` / column) | Meaning | Present when | Mutable |
| --- | --- | --- | --- |
| `ID` / `id` | internal id (UUIDv7) | always | no |
| `Origin` / `origin` | `INTERNAL` or `EXTERNAL` | always | no |
| `Kind` / `kind` | `OPENING`, `BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK` | always | no |
| `Status` / `status` | see [Transaction state machine](#transaction-state-machine) | always | yes |
| `WalletID`, `PlayerID` | wallet and player | always | no |
| `Money` / `amount_minor`, `currency` | amount | always | no |
| `ProviderID`, `ExternalTransactionID`, `IdempotencyKey`, `PayloadHash`, `RoundID`, `GameID` | external identity, idempotency and game context | `EXTERNAL` only | no |
| `ReferenceExternalTransactionID` | reference as sent by the provider | optional for `WIN`, mandatory for `REFUND`/`ROLLBACK` | no |
| `ReferenceTransactionID` / `reference_transaction_id` | resolved internal reference (`uuid.Nil` until resolved) | when resolved | yes |
| `FailureCode` | failure code | `REJECTED`, `FAILED` | yes |
| `ResultBalance` / `result_balance_minor` | balance returned to the provider (after processing, or observed at a rejection) | `PROCESSED` always; `REJECTED` when there is a balance to show | yes |
| `CorrelationID` | correlation id of the request or message | always | yes |
| `Attempts`, `NextAttemptAt`, `ExpiresAt` | reference retry bookkeeping | `PENDING_REFERENCE` | yes |
| `CreatedAt`, `UpdatedAt` | timestamps (UTC) | always | `UpdatedAt` only |

The guard trigger freezes every field marked "no", plus `created_at`; the shape checks of [OPENING](#opening) and the status checks define which fields may be `NULL` per origin and status.

## Transaction state machine

```
             ┌──────────────► PROCESSED
             │
  PENDING ───┼──────────────► REJECTED
             │
             ├──────────────► FAILED
             │
             └─► PENDING_REFERENCE ──┬──► PROCESSED
                    │     ▲          ├──► REJECTED
                    └─────┘          └──► FAILED
                  Reschedule
```

`PROCESSED`, `REJECTED` and `FAILED` are terminal; the domain refuses any transition out of them, and so does the database.

**How the domain validates it.**
Every transition goes through the private guard `Transaction.transition(to, now)`, which allows `PENDING → PROCESSED | REJECTED | FAILED | PENDING_REFERENCE` and `PENDING_REFERENCE → PROCESSED | REJECTED | FAILED`; anything else returns `wager.ErrInvalidTransition`, checkable with `errors.Is`.
The public methods add their own preconditions:

| Method | Target | Precondition |
| --- | --- | --- |
| `Process(resultBalance, referenceID, now)` | `PROCESSED` | valid result balance |
| `Reject(code, observedBalance, now)` | `REJECTED` | a definitive business code |
| `Fail(code, now)` | `FAILED` | a non-empty code |
| `AwaitReference(nextAttempt, expires, now)` | `PENDING_REFERENCE` | a valid schedule |
| `Reschedule(nextAttempt, now)` | stays `PENDING_REFERENCE`, `attempts + 1` | only from `PENDING_REFERENCE`, otherwise `ErrInvalidTransition` |

`wager.Settle` refuses to evaluate a transaction that is already terminal.
`Rehydrate` checks that the stored status parses and that its fields agree (a `PROCESSED` row has a result balance, `REJECTED`/`FAILED` have a failure code, `PENDING_REFERENCE` has its schedule) without replaying transitions.
In the database: a status `CHECK` lists the five values, `wager_transactions_status_shape` and `wager_transactions_pending_reference_schedule` repeat the shape rules, and the guard trigger freezes terminal rows, forbids `PENDING_REFERENCE → PENDING` and forbids `attempts` from decreasing.

**No asynchronous acceptance.**
Operations without dependencies are completed in the same SQL transaction in which they are recorded.
The `PENDING` state exists in the domain, but is never committed to the database on its own: `Submit` builds the transaction, settles it and only then inserts it, in its final status, together with the ledger entry and the outbox events.
So there is no "died after writing `PENDING`" window, and the only durable pending state is `PENDING_REFERENCE`, resumed by the worker of any instance (see [References not yet available](#references-not-yet-available)).

**Transient and permanent failures.**
Transient is anything that may succeed when retried without changing the input: every error `pgstore` classifies as `ErrTransient` (see [Persistence and transactions](#persistence-and-transactions)), plus a concurrent duplicate that exhausted its three attempts and a version conflict on the wallet row.
In that case the transaction is rolled back, nothing is stored, the API answers `503` and the SQS message goes back to the queue with backoff.
An unclassified error is `500 INTERNAL` over HTTP and a retry over SQS.
A permanent infrastructure failure is an invariant violation the domain considered valid: the database raised `ErrIntegrity` (a constraint fired where the code did not expect it).
`Submit` then records the operation as `FAILED / INVARIANT_VIOLATION` in its own transaction, for auditing and so that it is not retried forever, and completes the inbox entry when it came from SQS.
`Submit` returns that `FAILED` result with no error, so HTTP answers `500` with the transaction body and the SQS message is deleted (`Done`), not sent to the DLQ.
If the database refuses even the audit row, the integrity error is returned as permanent (DLQ with `INVARIANT_VIOLATION` over SQS, `500 INTERNAL` with the error envelope over HTTP, nothing stored); if writing the audit row fails for any other reason, the result is transient.
The pending-reference worker records `FAILED` only on `ErrIntegrity`; any other error is logged, the operation stays `PENDING_REFERENCE` and due, and the next cycle tries again.

## Operations and references

| Kind | Movement | Rules |
| --- | --- | --- |
| `BET` | debit | positive amount, sufficient balance; no reference |
| `WIN` | credit | positive amount; optional reference to a `BET` of the same round |
| `LOSS` | none | amount `"0.00"` in the wallet currency; no reference, no ledger entry, no version change |
| `REFUND` | credit | mandatory reference to a processed `BET`; same amount |
| `ROLLBACK` | opposite of the original | mandatory reference to a processed `BET`, `WIN` or `REFUND`; same amount |

**Reference agreement.**
The reference is resolved by `(providerId, referenceExternalTransactionId)`, always within the authenticated provider, and the resolved id is stored in `reference_transaction_id`.
The checks run in this order:

1. missing, `PENDING` or `PENDING_REFERENCE`: unavailable, the operation waits (see below);
2. not `PROCESSED`: `REFERENCE_NOT_PROCESSED`;
3. kind not allowed for the operation: `INVALID_REFERENCE_KIND`;
4. different provider, player, wallet, currency or round: `REFERENCE_MISMATCH` (one code for all five);
5. for `REFUND`/`ROLLBACK` only: amount different from the reference's, `AMOUNT_MISMATCH`, then an already compensated reference, `ALREADY_REVERSED`.

The `wager_transactions_reference_binding` trigger repeats every check of a processed operation in the database.

**`LOSS`.**
A processed `LOSS` emits only `WagerTransactionProcessed`: it has no ledger entry, so it emits no `WalletBalanceChanged`, and it leaves the wallet version unchanged.
`TestSettleLossHasNoMovement` and `TestLossAndRejectionsProduceNoLedger` cover the absence of the ledger entry and of the version change.

**`WIN` reference.**
The reference is optional; a `WIN` without one is credited directly, with no round or bet check.
When present, it may only point to a `BET`, and it goes through steps 1 to 4 above: a missing or still pending `BET` makes the `WIN` wait as `PENDING_REFERENCE` (`202`), then `REFERENCE_NOT_FOUND` after the TTL.
The amount is not compared, so a `WIN` can be any positive amount and `AMOUNT_MISMATCH` never applies to it.
The compensation state of the `BET` is ignored (`ALREADY_REVERSED` does not apply), and several `WIN`s may reference the same `BET`.

<a id="opening"></a>

**`OPENING`.**
`OPENING` is reserved for the internal wallet opening.
It is created only by `POST /wallets` (role `wallet-internal`) with a positive initial balance: in one commit, the wallet at version 1, the `OPENING` transaction born `PROCESSED`, its `CREDIT` ledger entry from zero to the initial balance at wallet version 1, and the `WagerTransactionProcessed` and `WalletBalanceChanged` outbox events.
A zero-balance opening writes only the wallet.
`OPENING` sent from outside is refused before anything is stored: over HTTP with `400 UNSUPPORTED_KIND`, and over SQS by sending the message to the DLQ with reason `UNSUPPORTED_KIND`, without touching the database or spending the 5 receives.
Its fields are the internal id, `walletId`, `playerId`, currency, a positive amount, status `PROCESSED`, the timestamps and `correlationId` equal to the transaction id; `providerId`, `externalTransactionId`, `idempotencyKey`, `payloadHash`, `roundId`, `gameId` and both reference columns are `NULL`.

The schema tells the two origins apart with the `origin` column (`INTERNAL`/`EXTERNAL`) and `CHECK wager_transactions_origin_shape`: an `INTERNAL` row must be `OPENING` with every provider and reference column `NULL`, and an `EXTERNAL` row must not be `OPENING` and has every provider column `NOT NULL`.
The external uniqueness indexes are partial (`WHERE origin = 'EXTERNAL'`), so internal rows with `NULL` fields never collide.
`wager_transactions_one_opening_per_wallet` (`UNIQUE (wallet_id) WHERE kind = 'OPENING'`) prevents a duplicate initial credit, and `UNIQUE (player_id, currency)` on `wallets` answers a repeated opening with `409 WALLET_ALREADY_EXISTS`.
The domain enforces the same rule (`Kind.External()`, `NewOpening` builds `Origin Internal`, `Rehydrate` rejects an origin that does not match the kind), and the `migrations/schema_integration_test.go` subtest "OPENING is internal only and unique per wallet" breaks both constraints as the application role.

### Reversals

Reversals are `REFUND` and `ROLLBACK`; both require `referenceExternalTransactionId`, otherwise `400 MISSING_REFERENCE`.

- `REFUND` resolves only a `BET` and credits it back.
- `ROLLBACK` resolves a `BET`, `WIN` or `REFUND` and moves in the opposite direction: it credits for a `BET` and debits for a `WIN` or `REFUND`; the ledger trigger requires that opposite direction.
- `ROLLBACK` of a `ROLLBACK`, and `REFUND` of anything other than a `BET`, are refused with `INVALID_REFERENCE_KIND`.
- Only the full referenced amount is accepted; partial reversals are out of scope, and a different amount is `REJECTED / AMOUNT_MISMATCH`, in the domain and in the reference-binding trigger.
- A reversal whose reference has not arrived waits as `PENDING_REFERENCE` (see [References not yet available](#references-not-yet-available)).

**`REFUND` and `ROLLBACK` combinations.**
A referenced transaction is successfully compensated at most once, by a `REFUND` or by a `ROLLBACK`, never by both.
A refunded `BET` cannot be rolled back, and a rolled-back `BET` cannot be refunded: both would return the same debit.
A `ROLLBACK` can undo a `REFUND`, debiting the amount again; in that case the original `BET` remains compensated (by the `REFUND`) and does not accept another refund, so the sequence `BET`, `REFUND`, `ROLLBACK(REFUND)` ends with the bet's debit standing, which is the consistent outcome.
Only a `PROCESSED` reversal uses up the reference: `IsReversed` and the unique index count only processed `REFUND`/`ROLLBACK` rows, so a rejected or failed reversal does not block a later one.
A `WIN` does not count as a compensation, and a `WIN` on a refunded `BET` is accepted.
The rule is enforced by the partial unique index `wager_transactions_one_reversal_per_reference`, so two concurrent reversals of the same reference cannot both succeed; `CHECK wager_transactions_processed_reversal_resolved` requires a processed reversal to carry its resolved reference.
`TestRefundRollbackCombinations` covers the sequences.

**Reversal without balance.**
The `ROLLBACK` of a `WIN` or of a `REFUND` debits the wallet.
If the balance does not cover it, the operation is rejected with `REVERSAL_INSUFFICIENT_FUNDS`, distinct from a bet's `INSUFFICIENT_FUNDS`, and is recorded for auditing.

### References not yet available

When the reference does not exist, the operation is stored as `PENDING_REFERENCE`, with `attempts`, `next_attempt_at` and `expires_at`, and the `WagerTransactionPendingReference` event goes to the outbox in the same commit.
The API answers `202`.
A database `CHECK` requires the schedule columns in that status, and the partial index `wager_transactions_pending_reference_due` on `next_attempt_at` backs the scan of due rows.

**Backoff.**
The first attempt is scheduled at `now + Delay(0)`, about the base delay, and expires at `now + TTL`.
Each unsuccessful attempt runs `Reschedule`, which increments `attempts` and sets `next_attempt_at = now + Delay(attempts)`.
`Delay(n)` doubles the base `n` times (factor 2) and caps it, then applies a uniform jitter of up to ±20%, never going above the cap.
The defaults are `PENDING_BASE_DELAY=1s`, `PENDING_MAX_DELAY=1m`, `PENDING_TTL=10m` and `PENDING_POLL_INTERVAL=500ms`; the configuration refuses a cap below the base.
When `expires_at` passes, the operation ends as `REJECTED` with `REFERENCE_NOT_FOUND` and emits `WagerTransactionRejected`.

**Worker and claiming.**
Every instance runs one `pending-reference` goroutine.
Each cycle has a 30 s deadline and reads up to 50 due rows ordered by `next_attempt_at`, with a plain `SELECT` in a read-only transaction; it sleeps for the poll interval only when fewer than 50 were handled.
Each row is then settled in its own transaction: the wallet first with a blocking `SELECT ... FOR UPDATE`, then the operation row with `FOR UPDATE SKIP LOCKED`, which re-checks `status = 'PENDING_REFERENCE'` and `next_attempt_at <= now`.
A row that another instance holds or already settled returns nothing and the attempt is a no-op, so two instances never settle the same operation.
They can still wait on the wallet row lock, bounded by `DB_LOCK_TIMEOUT`, like any writer of that wallet.
The settlement, the update of the row, the ledger entry and the outbox events share one commit.

**Restart and interruption.**
There is no in-memory queue: the schedule is the `next_attempt_at`, `expires_at` and `attempts` columns, and the worker re-reads due rows on every cycle.
The worker has no lease or claim column: its claim is the row lock held inside the settling transaction.
If an instance dies or stops mid-attempt, the transaction rolls back and the lock is freed with the connection; the row is still `PENDING_REFERENCE` and due, and the next poll of any instance, within `PENDING_POLL_INTERVAL`, picks it up.
That is why pending operations survive a restart of every instance (`TestRestartPreservesIdempotencyAndPendingReferences`).

**Wake-up.**
When the referenced operation is processed (a `BET`, `WIN` or `REFUND` reaching `PROCESSED`), the same commit brings forward the `next_attempt_at` of the operations waiting for it (`WakeWaiting`, with `FOR UPDATE SKIP LOCKED`), so they do not wait for the next backoff step.
Nothing is woken when the reference ends `REJECTED`.

| State of the reference | Behavior |
| --- | --- |
| does not exist | stays `PENDING_REFERENCE` until the TTL, then `REJECTED / REFERENCE_NOT_FOUND` |
| exists and is `PENDING_REFERENCE` | treated as still unavailable (see below) |
| exists and ended `REJECTED` or `FAILED` | `REJECTED / REFERENCE_NOT_PROCESSED`, immediately |
| exists, processed, but of a kind not allowed | `REJECTED / INVALID_REFERENCE_KIND` |
| exists, processed, but does not match | `REJECTED / REFERENCE_MISMATCH` or `AMOUNT_MISMATCH` |
| exists and was already compensated | `REJECTED / ALREADY_REVERSED` |

**Reference that exists but is still pending.**
Only a `ROLLBACK` of a `WIN` or `REFUND` that is itself `PENDING_REFERENCE` can hit this case: a `BET` has no reference, so it is never pending, and `REFUND`/`WIN` only reference a `BET`.
The dependent keeps its own `attempts`, backoff and `expires_at`, independent of the reference's, and the API and replays keep answering `202` for it.
If the reference is later processed, the same commit wakes the dependent, which then settles normally.
If the reference ends `REJECTED` (for example its own TTL expired), nothing wakes the dependent; its next scheduled attempt rejects it with `REFERENCE_NOT_PROCESSED`.
If the dependent's TTL passes while the reference is still pending, it ends `REJECTED / REFERENCE_NOT_FOUND` even though the reference row exists.
`WagerTransactionPendingReference` is emitted only on the first entry into the state, not on each reschedule.

## Concurrency

**Strategy: pessimistic per-wallet lock, with a version check.**
Each financial operation opens the transaction, reads the wallet with `SELECT ... FOR UPDATE`, lets the aggregate decide, writes the new balance with `UPDATE ... WHERE id = $1 AND version = $5` and checks that one row was affected.
The isolation level is `READ COMMITTED`: the second writer waits on the row lock and then reads the committed balance.
The lock is on that wallet's row; different wallets contend for nothing and move forward in parallel.
There is no global lock, no advisory lock or `LOCK TABLE`, no in-memory mutex coordinating wallets and no distributed lock outside the database.

**Why pessimistic and not optimistic.**
The central test is contention on the same wallet (two 80.00 bets against 100.00, or 50 copies of the same bet).
With optimistic control, almost every contention becomes a conflict and a retry; with a row lock, the second transaction waits a few milliseconds and decides with the already updated balance.
The aggregate needs the previous balance to decide and to build the ledger entry (`balance_before`), so a conditional `UPDATE` alone would move the business rule into SQL.

**Lost updates.**
Three barriers prevent them: the row lock; the version check in the `UPDATE`; and, in the database, `CHECK (balance_minor >= 0)` plus the deferred trigger that requires `version = OLD.version + 1` and a matching ledger entry for every balance change.
A version mismatch (0 rows affected) returns `ErrConcurrentUpdate`; it is not retried in process, it becomes `ErrTransient` (`503`, or the message back to the queue) and increments `wallet_lock_conflicts_total`.

**Retry limits.**
The only in-process retry is for `ErrDuplicate`, a concurrent copy that hit a unique index: the whole unit of work is retried up to three times, and the next attempt reads the committed copy.
When the attempts run out, the result is `ErrTransient` and the lock-conflict counter is incremented.
`lock_timeout`, deadlock and version conflicts are not retried in process; the bound on retries comes from the caller (`503` with `Retry-After`) or from SQS (5 receives, then the DLQ).

**Lock order and deadlock.**
Several rows are locked in a fixed order: the wallet row first, then rows of `wager_transactions`.
`Submit` locks the wallet and then, when it processes a reference, locks the operations waiting for it with `FOR UPDATE SKIP LOCKED`.
The pending-reference worker locks the wallet and then the pending operation with `FOR UPDATE SKIP LOCKED`, the same order as `Submit`.
The secondary locks use `SKIP LOCKED`, so they never wait, and reversal and reference belong to the same wallet; no lock cycle can form.
Every pool connection sets `lock_timeout` as a session runtime parameter from `DB_LOCK_TIMEOUT` (default 3 s), so it applies to every statement.
Exceeding it (`55P03`) or a deadlock (`40P01`) is classified as transient.

**Required race: 100.00 against two 80.00 bets.**
Exactly one bet is `PROCESSED` (`200`) and the other is `REJECTED / INSUFFICIENT_FUNDS` (`422`); the final balance is 20.00, with exactly one `DEBIT` ledger entry and no negative balance.
The first transaction to get the lock debits 80.00; the second waits, reads 20.00 and is rejected, writing no ledger entry and no balance change.
Resending either bet with the same key, on any instance, returns the original status with `idempotentReplay: true`: the stored `422` is never re-evaluated against a new balance, and the `200` never debits again.
`TestTwoBetsOfEightyOnOneHundred` (two services with separate pools) and `TestTwoBetsOfEightyOnDifferentInstances` (five rounds over the three Compose instances) assert exactly this, resends included.

**Proof.**
`internal/app/wagering/service_integration_test.go` runs the contention scenarios with independent services, each with its own pool: `TestSameBetFiftyTimesInParallel`, `TestTwoBetsOfEightyOnOneHundred` and `TestDistinctWalletsProgressInParallel`, which holds one wallet's lock in another transaction while 20 other wallets progress.
`e2e/e2e_test.go` repeats them against the three Compose instances; [Testing](#testing) maps every scenario to its test.

## Multiple instances

**How they run.**
`docker-compose.yml` defines three named services, `wallet-1`, `wallet-2` and `wallet-3`, not `deploy.replicas`.
They share the `x-wallet` YAML anchor and one image, `wallet-service:local`, built once by the `migrate` service (building the same tag from several services races in BuildKit) and used with `pull_policy: never`.
Each has its own `INSTANCE_ID` and host port (8091, 8092, 8093 → 8080), `restart: unless-stopped` and `stop_grace_period: 40s`, and starts only after `migrate` and `provision` completed successfully and Keycloak is healthy.

**What is separate per process.**
Each instance is its own process with its own memory and its own `pgxpool`, sized by `DB_MAX_CONNS` (default 10, minimum 2).
Postgres sees each pool as `application_name = wallet-<INSTANCE_ID>`.
`INSTANCE_ID` (default: the hostname) is the outbox lease owner (`locked_by`) and is added to every log line.

**What is shared.**
The same PostgreSQL database, the same SQS queues and SNS topic on MiniStack, and the same Keycloak JWKS.
The database is the only coordinator: row locks, unique indexes, the inbox, outbox leases and `SKIP LOCKED` claims.

**What is tested against them.**
The `e2e` build tag runs against the three instances (`E2E_INSTANCES`, default `http://localhost:8091,8092,8093`): the same bet spread across instances plus SQS copies, 80+80 against 100 on different instances, and distinct wallets across instances.
With `E2E_DOCKER=1` it also restarts every instance, sends `SIGKILL` to one in the middle of an SQS burst, stops one with `SIGTERM` under load and pauses Postgres.
[Testing](#testing) has the commands and the full scenario map, and [Failure matrix](#failure-matrix) the argument for correctness.

## Idempotency

**Keys.**
The `Idempotency-Key` header is mandatory over HTTP; over SQS the key is `data.idempotencyKey`.
The server never replaces the received key with a computed one.
The key is unique per provider (partial unique index on `(provider_id, idempotency_key) WHERE origin = 'EXTERNAL'`), and the financial operation is unique per `(provider_id, external_transaction_id)`, regardless of the key.

**Persistent, not in memory.**
Idempotency state lives only in PostgreSQL: the key and payload hash are columns of `wager_transactions`, the result is the stored row (`status`, `result_balance_minor`, `failure_code`), and the SQS inbox is the `inbox_messages` table.
There is no in-memory dedup or cache; the only in-process cache is the JWKS signing keys, unrelated to idempotency.
SQS FIFO deduplication is not relied on, and Postgres keeps its data in the named volume `pgdata`.
So idempotency survives a restart of every process: `TestRestartPreservesIdempotencyAndPendingReferences` restarts the three instances and replays a bet on another one, getting the same `transactionId`, `idempotentReplay: true` and the same balance.

**Payload hash.**
SHA-256 over the canonical JSON of the business fields, stored as `sha256:v1:<hex>`; the version prefix allows the algorithm to change without invalidating old hashes.
Canonical JSON here means keys in lexicographic order, no whitespace, UTF-8 strings and absent fields omitted (never `null`).

| Included in the hash | Excluded |
| --- | --- |
| `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money.amount`, `money.currency`, `referenceExternalTransactionId` (if present) | `Idempotency-Key`/`idempotencyKey`, `messageId`, `type`, `occurredAt`, headers, token, correlation id |

The only normalization is of the UUIDs (`playerId`, `walletId`), rewritten in canonical lower-case form before hashing.
They are parsed with `uuid.Parse` (`google/uuid`), which accepts more than the hyphenated form: any letter case, `{...}` braces, the `urn:uuid:` prefix and 32 hex digits without hyphens.
Every accepted form is rewritten through `uuid.String()` before hashing, so `{0192F28F-...}`, `urn:uuid:0192f28f-...` and `0192f28f5dc0...` for the same UUID produce the same hash and replay the same transaction instead of conflicting.
`POST /wallets` parses `playerId` the same way, and the path parameters of the reads also go through `uuid.Parse`.
The JSON is generated without HTML escaping (`<`, `>` and `&` stay literal), so that the hash does not depend on an `encoding/json` detail.
Monetary values are not normalized, because only the canonical form is accepted.
The currency code is case-sensitive and not upper-cased: a lower-case code such as `brl` is rejected with `INVALID_CURRENCY` (400 over HTTP), so only the canonical form reaches the hash.
HTTP and SQS both build the command with `wager.NewCommand`, which computes the hash, so the same operation produces the same hash on both channels by construction.

**Behavior.**

| Situation | HTTP | SQS |
| --- | --- | --- |
| new key | processes | processes |
| same key, same hash | stored result, `idempotentReplay: true`, same HTTP status as the original | replay: the inbox entry is completed with the original `transactionId` and the message is deleted |
| same key, different hash | `409 IDEMPOTENCY_KEY_REUSED` | DLQ, reason `IDEMPOTENCY_KEY_REUSED` |
| `externalTransactionId` already used with another key | `409 DUPLICATE_EXTERNAL_TRANSACTION` | DLQ, reason `DUPLICATE_EXTERNAL_TRANSACTION` |

**Replay.**
A replay is a read of the persisted transaction.
It returns before the wallet lock and before `Settle`, so it writes no transaction, ledger entry, balance or version change and emits no events; only the SQS inbox entry is completed.
The replay returns the balance stored at the original processing (`result_balance_minor`), not the current wallet balance.
A rejection also stores the balance observed at decision time, returned in `balance` for diagnosis.
The replay of an operation still in `PENDING_REFERENCE` returns its current state: `202` while it waits, and the final result (`200` or `422`) once the worker completes it.

**Across channels.**
An SQS copy of an operation already processed over HTTP (or the reverse) is answered as a replay of the same transaction, with no second ledger entry.
This needs the same idempotency key on both channels (the HTTP `Idempotency-Key` equal to the SQS `data.idempotencyKey`); a copy with another key and the same `externalTransactionId` is `DUPLICATE_EXTERNAL_TRANSACTION`.
`TestInboxAcrossChannels` (HTTP first, then SQS, then a redelivery, then a reused `messageId`) covers the sequential case and ends with exactly one debit.

**Simultaneous duplicates.**
The key lookup happens before the lock (replays do not wait for the wallet) and again after it, because a copy that committed while this one was waiting is now visible.
Under `READ COMMITTED` each statement sees its own snapshot, so a copy that commits between the key lookup and the `externalTransactionId` lookup shows up only in the second one; that is why the external id lookup checks the key before answering `DUPLICATE_EXTERNAL_TRANSACTION` (the 50-copy test found that window).
If two copies still reach the `INSERT`, the unique index refuses the second one, the transaction is rolled back and a new attempt reads the existing record, up to three times.
The result is a single ledger entry, proven by the 50-parallel-copies test.

**HTTP against SQS for the same operation.**
An HTTP request and an SQS message carrying the same operation can be processed at the same moment, on the same instance or on different ones.
The inbox does not deduplicate across channels, because it is keyed per message.
The arbiter is the `wager_transactions` row, through the two partial unique indexes: the losing side, on either channel, gets a `23505`, rolls back (over SQS, its inbox row too), retries and finds the committed row.
Same key and same hash is then a replay, and over SQS the inbox entry is completed with the winner's `transactionId`; a different hash is `IDEMPOTENCY_KEY_REUSED` and another key is `DUPLICATE_EXTERNAL_TRANSACTION`.
`TestHTTPAndSQSRaceForTheSameOperation` sends 10 HTTP submits and 10 SQS messages (distinct `messageId`s, same key) at once and asserts one debit and 10 completed inbox rows; `TestSameBetFiftyTimesInParallel` alternates channels over 50 goroutines; `TestSameBetFiftyTimesAcrossInstances` mixes 50 HTTP copies over three instances with 10 SQS copies.

## HTTP contract

Every response is JSON.
Two body shapes exist, and together with the status they tell every situation apart:

- the error envelope `{"error": {"code": "...", "message": "..."}}`, for `400`, `401`, `403`, `404`, `409`, `503` and `500 INTERNAL`;
- the transaction body `{transactionId, status, balance?, failureCode?, idempotentReplay}`, for `200`, `202`, `422` and `500` with `status: FAILED`.

The two kinds of `500` therefore differ by body.

| Status | When | Body |
| --- | --- | --- |
| `200` | operation processed, new or replay; every read | transaction or resource |
| `201` | wallet opened | wallet |
| `202` | operation accepted as `PENDING_REFERENCE` | transaction |
| `400` | invalid JSON, missing or unknown field, malformed value, missing `Idempotency-Key`, `OPENING` kind | error |
| `401` | missing, invalid or expired token (`UNAUTHENTICATED`) | error, plus `WWW-Authenticate` |
| `403` | insufficient role or `providerId` in body/path different from the token's (`FORBIDDEN`) | error |
| `404` | resource that does not exist or belongs to another provider, without confirming that it exists (`NOT_FOUND`, `WALLET_NOT_FOUND`) | error |
| `409` | `IDEMPOTENCY_KEY_REUSED`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS` | error |
| `422` | confirmed business rejection (`status: REJECTED`, with `failureCode`) | transaction |
| `500` | `status: FAILED` / `INVARIANT_VIOLATION` recorded for auditing | transaction |
| `500` | unexpected error or panic, or the database refusing even the `FAILED` audit row (`INTERNAL`) | error |
| `503` | transient unavailability; `TEMPORARILY_UNAVAILABLE` with `Retry-After: 1` | error |

`400` is correctable by the client and nothing is stored.
`422` is a definitive, stored result: resending the same operation returns the same `422` as a replay.
Every request has a deadline (`HTTP_REQUEST_TIMEOUT`, 10 s, database included): with the database frozen the response is `503` within the deadline, and resending with the same key after it comes back applies the operation once (`TestFrozenDatabaseAnswers503`).
`503` stores nothing and can be resent with the same key.

**Order of checks.**
Authentication (`401`) and role (`403`) run before the handler, so a refused request never opens a transaction.
`POST /wagering/transactions` then checks the `Idempotency-Key` (`400`), decodes the body (`400`), compares `providerId` with the token's (`403`), and only then validates money and the command; a body with a wrong `providerId` gets `403` even if other fields are invalid.

**401.**
A missing or non-Bearer token gets `WWW-Authenticate: Bearer` and the message `missing bearer token`; an invalid or expired token gets `WWW-Authenticate: Bearer error="invalid_token"` and `invalid or expired token`.

**Invalid input (`400`).**
The error code is in `error.code`; there is no transaction and no `idempotentReplay`.

```json
{"error": {"code": "INVALID_AMOUNT", "message": "amount must be a decimal string such as \"25.00\""}}
```

| Input | Code |
| --- | --- |
| `Content-Type` other than `application/json`, body over 64 KiB, unknown field, invalid JSON, trailing data after the object | `INVALID_REQUEST` |
| missing `Idempotency-Key` header, missing `money` or `initialBalance` | `INVALID_REQUEST` |
| identifier empty, over 128 bytes, with whitespace or non-printable characters (`providerId`, `externalTransactionId`, `idempotencyKey`, `roundId`, `gameId`, `referenceExternalTransactionId`) | `INVALID_REQUEST` |
| `playerId` or `walletId` not a non-nil UUID in a form `uuid.Parse` accepts (see [Idempotency](#idempotency)) | `INVALID_REQUEST` |
| reference on `BET` or `LOSS`; an operation referencing itself | `INVALID_REQUEST` |
| amount as a JSON number, not canonical or negative; `LOSS` not `0.00`; another kind not positive | `INVALID_AMOUNT` |
| unsupported currency | `INVALID_CURRENCY` |
| unknown kind or `OPENING` | `UNSUPPORTED_KIND` |
| `REFUND`/`ROLLBACK` without reference | `MISSING_REFERENCE` |
| invalid ledger cursor, `limit` outside 1–200 | `INVALID_REQUEST` |

A malformed UUID in a path is `404 NOT_FOUND`, not `400`.

**Pending processing (`202`).**

```json
{"transactionId": "0192f2a0-...", "status": "PENDING_REFERENCE", "idempotentReplay": false}
```

There is no `balance` and no `failureCode`; `idempotentReplay` is `true` on a replay while still pending.
The client follows the operation with `GET /wagering/transactions/:transactionId` (which shows `attempts`, `nextAttemptAt` and `expiresAt`) or by replaying with the same key, which returns `200` or `422` once the worker settles it.

**Business rejection (`422`).**

```json
{"transactionId": "0192f2a0-...", "status": "REJECTED", "balance": {"amount": "20.00", "currency": "BRL"}, "failureCode": "INSUFFICIENT_FUNDS", "idempotentReplay": false}
```

`balance` is absent for `PLAYER_WALLET_MISMATCH` and `CURRENCY_MISMATCH`.

**Transient unavailability (`503`).**

```json
{"error": {"code": "TEMPORARILY_UNAVAILABLE", "message": "temporary failure, retry with the same Idempotency-Key"}}
```

The `Retry-After` header is always `1` (second).
Causes: every `ErrTransient` (database down or frozen, request deadline, `lock_timeout`, deadlock, serialization failure, too many connections), a concurrent duplicate that exhausted its attempts, a version conflict on the wallet row, and a failure while recording a `FAILED` audit row.
Readiness has its own `503` body, described in [Observability](#observability).

### POST /wallets

Role `wallet-internal`.
Request: `{"playerId": "<uuid>", "initialBalance": {"amount": "1000.00", "currency": "BRL"}}`; `initialBalance` is required and must be non-negative.
Response `201`: `{"id", "playerId", "balance": {"amount", "currency"}, "version", "createdAt", "updatedAt"}`, timestamps in UTC.
With a positive balance, the wallet at version 1, the `OPENING` transaction in `PROCESSED`, its credit ledger entry and the `WagerTransactionProcessed` and `WalletBalanceChanged` outbox events are written in one commit; see [OPENING](#opening).
A second opening for the same player and currency is `409 WALLET_ALREADY_EXISTS`; the endpoint is not idempotent by key.

### POST /wagering/transactions

Role `wagering-provider`; the `Idempotency-Key` header is mandatory.

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "refund-1",
  "playerId": "0192f291-...",
  "walletId": "0192f291-...",
  "roundId": "round-1",
  "gameId": "game-1",
  "kind": "REFUND",
  "money": {"amount": "25.00", "currency": "BRL"},
  "referenceExternalTransactionId": "bet-1"
}
```

`referenceExternalTransactionId` is the reversal field: mandatory for `REFUND`/`ROLLBACK`, optional for `WIN` and refused on `BET`/`LOSS`.
Unknown fields are rejected and the body is capped at 64 KiB.
Response: `{"transactionId", "status", "balance"?, "failureCode"?, "idempotentReplay"}`, with `200` (`PROCESSED`), `202` (`PENDING_REFERENCE`), `422` (`REJECTED`) or `500` (`FAILED`); a replay uses the original code, and `balance` is the stored result balance.

### Reads

All reads run in a `REPEATABLE READ READ ONLY` transaction and answer `200`; a missing wallet or transaction is `404 NOT_FOUND`.

| Endpoint | Role | Body |
| --- | --- | --- |
| `GET /wallets/:walletId` | `wallet-internal` | the same wallet body as `POST /wallets` |
| `GET /wallets/:walletId/ledger?cursor=&limit=` | `wallet-internal` | `{walletId, items: [{id, transactionId, direction, money, balanceBefore, balanceAfter, walletVersion, createdAt}], nextCursor?}` |
| `GET /wagering/transactions/:transactionId` | `wagering-provider` or `wallet-internal` | full transaction |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | `wagering-provider` | full transaction |

The full transaction body is `{transactionId, providerId?, externalTransactionId?, walletId, playerId, roundId?, gameId?, kind, money, status, failureCode?, balance?, referenceExternalTransactionId?, referenceTransactionId?, attempts, nextAttemptAt?, expiresAt?, createdAt, updatedAt}`; fields marked `?` are omitted when empty.
A read answers `200` whatever the stored status (`PROCESSED`, `REJECTED`, `FAILED` or `PENDING_REFERENCE`); only the submit maps the status to `202`, `422` or `500`.
`nextAttemptAt` and `expiresAt` are present while `PENDING_REFERENCE`, which is how a client follows a pending operation.
The internal role sees any transaction, `OPENING` included; a provider sees only its own, and anything else is `404`.
On the by-external-id route, a path `providerId` different from the token's is `403`.

**Ledger pagination.**
Keyset pagination on `seq`, an identity column (`GENERATED ALWAYS AS IDENTITY`) backed by the index `wallet_ledger_entries_wallet_seq` on `(wallet_id, seq)`.
Because `seq` only grows and the ledger is append-only, the order is total and stable, and a page never skips or repeats an entry.
The cursor is `base64url("seq:<n>")` without padding, opaque to the client; a request without a cursor starts at the first entry, and the query is `seq > cursor ORDER BY seq LIMIT limit`.
`limit` defaults to 50 and goes up to 200.
The handler fetches `limit + 1` rows; `nextCursor` is present only when there is a next page and absent on the last one.
The wallet is checked first, so a missing wallet is `404`.

**Reconciliation.**
`POST /wallets/:walletId/reconciliation` (role `wallet-internal`) reads the stored balance and rebuilds it in the same `REPEATABLE READ READ ONLY` transaction.
The rebuilt balance is the signed sum, `CREDIT` minus `DEBIT`, of every ledger entry of the wallet, the `OPENING` credit included; a wallet opened with zero has no opening entry, so the sum starts at zero.
The sum runs in `numeric`, so it cannot overflow `bigint`.
The response is always `200`: `{walletId, storedBalance, calculatedBalance, difference, consistent, checkedEntries}`, where `difference = stored − rebuilt`, `consistent` means `difference == 0` and `checkedEntries` is the number of ledger entries summed.
It never modifies the balance; the transaction is `READ ONLY`, so the database would refuse a write.
A divergence is reported three ways: `consistent: false` with the `difference` in the response, an `ERROR` log `reconciliation divergence` with `walletId`, `storedBalance`, `calculatedBalance`, `difference` and `checkedEntries`, and the label-less counter `wallet_reconciliation_divergences_total`, registered at 0 so it exists before the first divergence.

### Failure codes

`failureCode` appears in the transaction body only for `422` and `FAILED`; the correctable codes are returned in `error.code` of the error envelope.

| Code | Kind | Meaning | HTTP | SQS |
| --- | --- | --- | --- | --- |
| `INVALID_REQUEST` | correctable | malformed body or missing required field | `400` | DLQ, reason = code |
| `INVALID_AMOUNT` | correctable | amount not in canonical form, negative or overflowing | `400` | DLQ, reason = code |
| `INVALID_CURRENCY` | correctable | unsupported currency | `400` | DLQ, reason = code |
| `UNSUPPORTED_KIND` | correctable | unknown kind or `OPENING` coming from outside | `400` | DLQ, reason = code |
| `MISSING_REFERENCE` | correctable | `REFUND`/`ROLLBACK` without `referenceExternalTransactionId` | `400` | DLQ, reason = code |
| `WALLET_NOT_FOUND` | correctable | wallet does not exist; not stored, because there is no wallet to reference | `404` | retried; redrive to the DLQ after 5 receives |
| `PLAYER_WALLET_MISMATCH` | definitive | the wallet does not belong to the given player; the response carries no balance | `422` | stored, message deleted |
| `CURRENCY_MISMATCH` | definitive | operation currency different from the wallet's; the response carries no balance | `422` | stored, message deleted |
| `BALANCE_LIMIT_EXCEEDED` | definitive | the credit would push the balance past the `int64` limit | `422` | stored, message deleted |
| `INSUFFICIENT_FUNDS` | definitive | bet larger than the balance | `422` | stored, message deleted |
| `REVERSAL_INSUFFICIENT_FUNDS` | definitive | reversal that would debit more than the balance | `422` | stored, message deleted |
| `REFERENCE_NOT_FOUND` | definitive | reference did not arrive within the TTL | `422` | stored, message deleted |
| `REFERENCE_NOT_PROCESSED` | definitive | reference ended without success | `422` | stored, message deleted |
| `REFERENCE_MISMATCH` | definitive | provider, player, wallet, currency or round differ from the reference | `422` | stored, message deleted |
| `AMOUNT_MISMATCH` | definitive | reversal amount different from the referenced one | `422` | stored, message deleted |
| `INVALID_REFERENCE_KIND` | definitive | reference kind not allowed for the operation | `422` | stored, message deleted |
| `ALREADY_REVERSED` | definitive | the reference was already compensated | `422` | stored, message deleted |
| `INVARIANT_VIOLATION` | permanent failure (`FAILED`) | the database refused a state the domain accepted | `500` | stored, message deleted; DLQ only if the audit row is refused too |

The codes are stable: they are part of the contract and do not change meaning.
The HTTP-only codes are `UNAUTHENTICATED`, `FORBIDDEN`, `NOT_FOUND`, `IDEMPOTENCY_KEY_REUSED`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS`, `TEMPORARILY_UNAVAILABLE` and `INTERNAL`.
The SQS-only DLQ reasons are listed in [SQS consumer](#sqs-consumer).

## SQS consumer

**Queues.**
`wager-transactions.fifo` with redrive to `wager-transactions-dlq.fifo` after 5 receives (`maxReceiveCount`), a 30 s visibility timeout and 14-day retention on the DLQ.
Every queue is FIFO with `ContentBasedDeduplication=false`, so the broker deduplicates only by an explicit `MessageDeduplicationId`.

**Queue initialization.**
`cmd/provision` creates everything idempotently, in this order: the DLQ; the main queue, then `SetQueueAttributes` with the visibility timeout and the redrive policy (so a queue that already exists stays current, since `CreateQueue` with different attributes would fail); the SNS FIFO topic; and the `wallet-events-audit.fifo` queue with its raw-delivery subscription to the topic.
Names come from `WAGER_QUEUE_NAME`, `WAGER_DLQ_NAME` and `EVENTS_TOPIC_NAME` (defaults `wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wallet-events.fifo`).
In Compose it runs as the one-shot `provision` service once MiniStack is healthy, retrying up to 30 times 2 s apart within a 2-minute timeout while the emulator boots; the wallet instances wait for it with `service_completed_successfully`.
On start, each instance resolves the queue URLs and the topic ARN (an idempotent `CreateTopic`), and a missing queue fails the start; with `AWS_AUTO_PROVISION=true` it runs the full provisioning first.
The integration tests provision their own queues through the same function.

**`MessageGroupId` and `MessageDeduplicationId`.**
The producer uses `walletId` as the `MessageGroupId`: messages of the same wallet arrive in order and one at a time, and different wallets are consumed in parallel.
The `MessageDeduplicationId` is the envelope's `messageId`.
SQS FIFO deduplication lasts only 5 minutes and is not a correctness guarantee; the inbox and the database idempotency are.

**Envelope.**

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "bet-1",
    "idempotencyKey": "provider-a:bet-1",
    "playerId": "0192f291-...",
    "walletId": "0192f291-...",
    "roundId": "round-1",
    "gameId": "game-1",
    "kind": "BET",
    "money": {"amount": "25.00", "currency": "BRL"},
    "referenceExternalTransactionId": "..."
  }
}
```

`type` must be `WagerTransactionRequested`; `messageId` must be non-empty and at most 128 bytes; `occurredAt` is decoded but not validated or used.
Unknown fields in the envelope are ignored, so that producers can add metadata; the business fields go through `wager.NewCommand`, the same validation as HTTP.

**Durable identity.**
The inbox identity is the envelope `messageId`, never the SQS-assigned `MessageId`: a producer resend gets a new SQS `MessageId`, and FIFO dedup lasts only 5 minutes.
The SQS `MessageId` is used only in logs (`sqsMessageId`) and for the DLQ copy.

**Inbox.**
`inbox_messages (consumer_name, message_id, payload_hash, transaction_id, received_at, completed_at)` with `PRIMARY KEY (consumer_name, message_id)`; the consumer name is `wallet.wager-transactions`.
`payload_hash` is the business payload hash of the command, the same one HTTP idempotency uses, so a redelivery with another `occurredAt` and the same business data is a redelivery.
`transaction_id` and `completed_at` are `NULL` until the entry completes; once completed, a trigger forbids any change and any deletion, and the key, hash and `received_at` never change.
Registration is `INSERT ... ON CONFLICT (consumer_name, message_id) DO NOTHING`; a concurrent delivery of the same message waits on the primary key until the first one commits or rolls back.
The inbox is used only by SQS; HTTP relies on the idempotency keys alone.
Inbox and outbox rows are never deleted: the triggers forbid it and there is no cleanup job.

**Processing a message.**
1. Validates the envelope and builds the command; an invalid message is a permanent failure.
2. Opens the transaction and registers `(consumerName, messageId, hash)` in the inbox.
3. If the `messageId` already exists with a different hash, it is a permanent failure (`MESSAGE_ID_REUSED`), whether or not the earlier message completed.
4. If the message was already completed with the same hash, it is a redelivery: the stored transaction is returned as a replay, with no wallet lock and no new writes, and the message is deleted.
5. Calls the same use case as HTTP, inside the same transaction.
6. Marks the inbox entry as completed with the `transactionId` (the original one, on a replay found by key) and commits.
7. Only after the commit deletes the message from the queue.

A confirmed business rejection is a final result: the message is deleted.
An operation that enters `PENDING_REFERENCE` also completes the message; the worker takes over from there.
`WALLET_NOT_FOUND` is treated as transient over SQS (the wallet may be opened later): the message goes back to the queue and, after the 5 receives are used up, the redrive moves it to the DLQ.
Within a FIFO batch, when a message of a group goes back to the queue, the following ones of the same group go back with it, so the group keeps its order.

**Delete after commit.**
The message is deleted only after the commit of its handling.
If `DeleteMessage` fails while the process stays up, the failure is only logged (`sqs delete failed; the message will be redelivered and deduplicated`); there is no retry.
The message comes back after the visibility timeout, the inbox recognizes the completed entry and the message is deleted then, with nothing applied twice.
A redelivery still counts toward `maxReceiveCount`, so a message whose delete fails five times would reach the DLQ even though it was committed; the inbox makes that harmless.
`TestCrashAfterCommitBeforeDelete` makes the delete fail after the commit, forces the visibility to zero and checks that a second consumer recognizes the message and the debit stays at one.

**Retry and DLQ.**
Transient failure: the transaction is rolled back, the message visibility is set to `min(2^receiveCount, 30)` s (exponent capped at 5, so 2, 4, 8, 16, 30) and SQS delivers it again; after 5 receives, the redrive moves it to the DLQ.
A permanent failure is sent to the DLQ with the reason and deleted from the main queue, without spending the 5 attempts:

| DLQ `reason` | Cause | Touches the database |
| --- | --- | --- |
| `INVALID_MESSAGE` | malformed JSON (including an amount sent as a JSON number), empty or oversized `messageId` | no |
| `UNSUPPORTED_MESSAGE_TYPE` | `type` other than `WagerTransactionRequested` | no |
| `INVALID_REQUEST`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `UNSUPPORTED_KIND`, `MISSING_REFERENCE` | missing `money` or a field validation failure, `OPENING` included | no |
| `PROVIDER_NOT_AUTHORIZED` | the sender may not speak for the message's provider | no |
| `IDEMPOTENCY_KEY_REUSED`, `DUPLICATE_EXTERNAL_TRANSACTION`, `MESSAGE_ID_REUSED` | idempotency conflict | rolled back |
| `INVARIANT_VIOLATION` | the database refused even the `FAILED` audit row | rolled back |
| any other domain failure code | defensive default for typed errors | rolled back |

Invalid messages are not recorded in the database: no inbox row and no transaction row.
They show up only as the DLQ copy (attributes `reason`, `sourceQueue`, `failedAt`, `originalMsgId`; `MessageGroupId` the original group or `invalid`; `MessageDeduplicationId` the SQS `MessageId`), an `ERROR` log `sqs message sent to DLQ` and `wallet_sqs_dlq_total{reason}`.
If sending to the DLQ fails, the message is not deleted; it stays in the main queue and the redrive policy applies.
If sending to the DLQ and deleting are interrupted halfway, the message may appear twice in the DLQ; this is accepted, because the DLQ is inspected by people.
A failed `ChangeMessageVisibility` is logged, and the message reappears after the visibility timeout.

**Receive failures.**
When `ReceiveMessage` fails (SQS down), the loop logs and backs off from 1 s, doubling up to 10 s, resetting on success; on `QueueDoesNotExist` with `AWS_AUTO_PROVISION=true` it reprovisions first.

**Deadline and visibility.**
`ReceiveMessage` asks for up to 10 messages with a long poll of `SQS_WAIT_TIME` (default 10 s), and each instance runs `SQS_WORKERS` loops (default 4).
Each message has a processing deadline, `SQS_MESSAGE_TIMEOUT` (default 10 s), which the configuration requires to be positive and below the 30 s visibility timeout.
The visibility clock of a whole batch starts at receive, and the messages of a batch are handled one after another, so only about the first two or three messages of a full, slow batch are guaranteed to finish before their visibility expires.
There is no heartbeat or visibility extension.
If visibility expires mid-batch, another consumer may receive the message; the receive count goes up, and the inbox primary key serializes the two deliveries, so nothing is applied twice.

**Shutdown.**
On `SIGTERM`, the consumer stops calling `ReceiveMessage`, finishes the message in progress (its deadline is not cancelled by the shutdown) and returns the messages of the batch that have not started with `ChangeMessageVisibility` set to zero, for immediate redelivery to another instance.
A `ReceiveMessage` interrupted by the shutdown may already have delivered messages the process never sees; they come back after the visibility timeout.
A message interrupted after the commit and before the deletion is delivered again and the inbox recognizes it as completed.

**Broker credentials and policies.**
Not provisioned: the repository defines and versions no queue access policy (`Policy` attribute), no IAM principals and no policy JSON for producers versus the consumer.
`internal/platform/awsclient/provision.go` sets only `VisibilityTimeout`, `MessageRetentionPeriod`, `ContentBasedDeduplication` and `RedrivePolicy`, and `.env.example` has a single dummy credential pair shared by producers and the consumer.
The intended AWS design: producers and consumer would use distinct IAM principals, with the queue policy allowing `SendMessage` only to producers and `ReceiveMessage`/`DeleteMessage`/`ChangeMessageVisibility` only to the service.
The consumer still validates everything HTTP validates; the policy decides who talks to the queue, not what the message can do.
The message's `providerId` is checked against the sender (the SQS `SenderId`) through the `SQS_PROVIDER_SENDERS` mapping (`senderId=providerId,...`): when it is configured, a message from a sender that cannot speak for that provider goes to the DLQ with `PROVIDER_NOT_AUTHORIZED`, without touching the database.
Locally the mapping is empty, because MiniStack does not enforce IAM and returns the same `SenderId` (`000000000000`) for any credential (verified); the rule has a unit test, but the broker policy cannot be demonstrated on the emulator.

## Outbox and publishing

Events are written to `outbox_events` in the same transaction as the operation that originates them.
No event is published before the commit, because the publisher only reads already committed records.

**Schema.**

| Column | Meaning |
| --- | --- |
| `seq` (`bigint` identity, unique) | claim order |
| `id` (`uuid`, primary key) | the stable `eventId`, also the SNS `MessageDeduplicationId` |
| `aggregate_type`, `aggregate_id` | always `wallet` and the `walletId`, the SNS `MessageGroupId` |
| `event_type`, `event_version` | event name and version (`>= 1`) |
| `payload` (`jsonb`) | the full envelope, sent as the SNS message |
| `occurred_at` | creation time |
| `attempts` | claims so far (`>= 0`) |
| `next_attempt_at` (not null) | when the event is due; equal to `occurred_at` on insert |
| `locked_by`, `locked_until` | lease owner (`INSTANCE_ID`) and expiry |
| `published_at` | `NULL` until published |
| `last_error` | last failure, truncated to 500 characters |

The partial index `outbox_events_due` on `(next_attempt_at, seq) WHERE published_at IS NULL` backs the claim.
A trigger makes `id`, the aggregate, the type, the version, the payload and `occurred_at` immutable and forbids changing `published_at` once set; another trigger forbids `DELETE`.

**Publisher.**
Runs on every instance, as one goroutine.
On each cycle, it claims a batch in a single autocommit statement:

```sql
UPDATE outbox_events
SET locked_by = $instance, locked_until = now() + $lease, attempts = attempts + 1
WHERE id IN (
    SELECT id FROM outbox_events
    WHERE published_at IS NULL AND next_attempt_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY seq
    FOR UPDATE SKIP LOCKED
    LIMIT $batch)
RETURNING ...;
```

The lease is `OUTBOX_LEASE` (default 30 s) and the batch `OUTBOX_BATCH_SIZE` (default 50, allowed 1–1000).
After the claim, it splits the batch into chunks of 10 (the SNS `PublishBatch` limit), publishes the chunks in parallel, and confirms every accepted event with one statement: `UPDATE ... SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL WHERE id = ANY($1) AND locked_by = $instance AND published_at IS NULL`.
An entry SNS rejects, or every entry of a chunk whose call failed, is released one by one: `next_attempt_at` is moved forward, the lease is cleared and `last_error` stored, under `WHERE id = $1 AND locked_by = $instance AND published_at IS NULL`, so a stale publisher cannot touch a claim another instance took over.
A full batch is followed at once by the next claim; a partial or empty one waits `OUTBOX_POLL_INTERVAL` (default 200 ms); a failed claim (database down) is logged and backs off from the poll interval, doubling up to 5 s.
Each cycle's work is bounded by `lease / 2` (15 s by default), so it finishes before the lease expires.
`TestConcurrentPublishers` has two publishers compete for 120 events and checks that each is claimed once and received once.

**Backoff.**
`attempts` counts claims: it goes up at claim time, so a claim lost with its lease also counts.
After a failure, the delay is uniform in `[d/2, d]` with `d = min(1 s × 2^min(attempts, 9), 5 min)`: 1–2 s after the first failed claim, then doubling, up to 2.5–5 min.
There is no maximum number of attempts, no dead-letter state and no poison handling: an event that keeps failing is retried forever at the 5-minute cap, and it can never be dropped (no `DELETE`) or reopened once published.
`wallet_outbox_lag_seconds`, `wallet_outbox_backlog`, `wallet_outbox_publish_total{result="failed"}` and `last_error` are how such an event is noticed.
`TestUnavailableBrokerBacksOff` covers the backoff.

**Recovery.**
Every path to a repeated publication keeps the same `eventId`, because the `eventId` is the row's primary key, fixed at insert, and the payload is immutable:

| Failure point | What happens | Test |
| --- | --- | --- |
| crash after the business commit, before any claim | the row is unpublished and unleased; the next claim of any instance takes it | `TestConcurrentPublishers`, `TestAbandonedClaimIsTakenOverAfterTheLease` (both commit their events before any publisher runs) |
| crash after the claim, before publishing | the lease expires (30 s) and another instance claims and publishes | `TestAbandonedClaimIsTakenOverAfterTheLease` |
| SNS accepted the batch but the response was lost or failed | the events are rescheduled with backoff and published again | `TestRepublicationKeepsTheEventID` |
| the confirmation `UPDATE` fails, or the process dies before it | the lease expires and the events are published again | (same mechanism) |
| the lease expires while a publish is still running | another instance claims and publishes; only the lease holder can confirm | conditional `UPDATE ... WHERE locked_by = $owner AND published_at IS NULL` on confirm and release; not tested directly |

`TestRepublicationKeepsTheEventID` injects a lost acknowledgement and checks that the audit queue receives only the original two `eventId`s, with `attempts = 2`; `TestPublishedEventsCannotBeReopened` checks the trigger.
SNS FIFO drops a repetition within 5 minutes through `MessageDeduplicationId = eventId`; outside that window, the contract is at-least-once and the consumer deduplicates by `eventId`.

**Destination and provisioning.**
SNS FIFO topic `wallet-events.fifo` (`EVENTS_TOPIC_NAME`), created with `FifoTopic=true` and `ContentBasedDeduplication=false`, so dedup relies only on the explicit id.
It is created by `cmd/provision` (see [SQS consumer](#sqs-consumer)) together with the `wallet-events-audit.fifo` queue, which subscribes with raw delivery and is used for inspection and by the tests.

**Routing contract.**
Each SNS message carries `Message` = the stored envelope, `MessageGroupId` = `aggregateId` (the `walletId`), `MessageDeduplicationId` = `eventId`, and the message attribute `eventType` (`DataType: String`, one of the four event names) for subscription filters.
There is no publication order guarantee, not even with a single publisher: the chunks of a claim are published in parallel, and a failed event is retried later while later events of the same wallet go out.
SNS FIFO keeps order only within one `PublishBatch` call of the same group.

**Consumer contract.**
- Delivery is at-least-once; deduplicate by `eventId`.
- Order `WalletBalanceChanged` by `walletVersion`: it grows by exactly 1 per balance change, so a gap is detectable; the first value is 1 for a wallet opened with a balance and 2 for one opened with zero, and `LOSS` emits no `WalletBalanceChanged`.
- The other events carry no sequence; correlate them by `transactionId`, and treat `occurredAt` and the time-ordered `eventId` (UUIDv7) only as hints.
- Every event is `version: 1` today; changes within a version are additive, and a breaking change gets a new version or a new `eventType`.
- Ignore unknown fields, unknown `eventType`s and unknown versions; optional fields are omitted, never `null`.

**Envelope.**

```json
{
  "eventId": "0192f2a0-...",
  "eventType": "WalletBalanceChanged",
  "aggregateId": "0192f291-...",
  "correlationId": "msg-123",
  "causationId": "msg-123",
  "occurredAt": "2026-09-08T12:00:00.123Z",
  "version": 1,
  "data": {
    "walletId": "0192f291-...",
    "transactionId": "0192f298-...",
    "direction": "DEBIT",
    "money": { "amount": "25.00", "currency": "BRL" },
    "balanceBefore": { "amount": "1000.00", "currency": "BRL" },
    "balanceAfter": { "amount": "975.00", "currency": "BRL" },
    "walletVersion": 2
  }
}
```

| Field | Content |
| --- | --- |
| `eventId` | UUIDv7, the outbox primary key |
| `eventType` | one of the four types below |
| `aggregateId` | the `walletId`, for every event (aggregate type `wallet`) |
| `correlationId` | HTTP: the `X-Correlation-Id` header, or a generated UUID when it is missing or over 128 characters (echoed in the response header); SQS: the envelope `messageId`; `OPENING`: the transaction id; pending resume: the original request's correlation id |
| `causationId` (optional) | HTTP: the correlation id; SQS: the `messageId`; pending resume: the waiting transaction's id; `OPENING`: omitted |
| `occurredAt` | UTC, RFC 3339 with milliseconds |
| `version` | always 1 today |
| `data` | typed per `eventType` |

Each event has its own Go type, and the constructor sets `eventType` and `version`.
The payload is serialized once, when the event is created, and stored as an immutable snapshot.
The column is `jsonb`: the content is preserved and protected against changes, but the key order and whitespace of the published text are those of `jsonb`, not of the original serialization; consumers read JSON, they do not compare bytes.
Monetary values are `{amount, currency}` with the amount as a decimal string.

**Event catalog.**

| Event | Trigger | `data` |
| --- | --- | --- |
| `WagerTransactionProcessed` | operation completed successfully, including `LOSS` and `OPENING` | `transactionId`, `walletId`, `playerId`, `providerId`?, `externalTransactionId`?, `roundId`?, `gameId`?, `kind`, `money`, `balance` (resulting), `referenceTransactionId`? |
| `WagerTransactionRejected` | any stored rejection: business rule, mismatch, insufficient funds, TTL expiry (`REFERENCE_NOT_FOUND`) | `transactionId`, `walletId`, `playerId`, `providerId`, `externalTransactionId`, `kind`, `money`, `failureCode` |
| `WalletBalanceChanged` | one per ledger entry, so never for `LOSS`, rejections or pending operations | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion` |
| `WagerTransactionPendingReference` | first entry into `PENDING_REFERENCE` only, not each reschedule | `transactionId`, `walletId`, `providerId`, `externalTransactionId`, `kind`, `money`, `referenceExternalTransactionId`, `expiresAt` |

A processed operation with a movement emits `WagerTransactionProcessed` and `WalletBalanceChanged` in the same commit; `FAILED` emits no event.

**Internally originated events.**
`OPENING` events leave out the external metadata that does not apply: the `WagerTransactionProcessed` payload has no `providerId`, `externalTransactionId`, `roundId`, `gameId` or `referenceTransactionId` keys (absent, not `null`), and the envelope has no `causationId`, because there is no external message.
`WalletBalanceChanged` has no provider fields for any kind.
The database enforces the same shape (`wager_transactions_origin_shape`), and `TestOpeningEventsAndEnvelope` checks the omitted keys.

## Failure matrix

**Delivery assumption.**
The service assumes at-least-once delivery everywhere: every inbound HTTP request and SQS message may arrive more than once, on any instance, and every outbound event may be published more than once.
Nothing relies on exactly-once delivery or on the broker's 5-minute dedup window.

**Why the money stays correct.**
Each operation writes the transaction, the wallet, the ledger entry, the inbox entry and the outbox events in one commit, so there is no partial state to recover.
The database is the only coordinator: per-wallet row locks, unique indexes for idempotency, the inbox for messages, leases and `SKIP LOCKED` for background work.
Every in-flight step is either rolled back by Postgres when its connection drops or durable and recognized on retry.

| Situation | Duplicate movement prevented by | Negative balance prevented by | Loss of a committed event prevented by | Proof |
| --- | --- | --- | --- | --- |
| same operation received repeatedly, over HTTP and SQS | unique `(provider, key)` and `(provider, external id)`, payload hash, inbox, `UNIQUE (wallet_id, transaction_id)` on the ledger, retry on `23505` | not applicable | outbox in the same commit | `TestSameBetFiftyTimesInParallel`, `TestInboxAcrossChannels`, `TestHTTPAndSQSRaceForTheSameOperation`, `TestRedeliveryIsDeduplicatedByTheInbox`, `TestSameBetFiftyTimesAcrossInstances` |
| reversal before its reference | `wager_transactions_one_reversal_per_reference` | balance check under the row lock (`REVERSAL_INSUFFICIENT_FUNDS`) | `PENDING_REFERENCE` row and its event in the same commit; worker on every instance | `TestReversalBeforeReferenceIsResolvedByWorker`, `TestPendingReferenceExpires`, `TestRestartPreservesIdempotencyAndPendingReferences` |
| concurrent operations on the same wallet | row lock plus re-lookup after the lock | `SELECT ... FOR UPDATE`, version check, `CHECK (balance_minor >= 0)` | single commit | `TestTwoBetsOfEightyOnOneHundred`, `TestTwoBetsOfEightyOnDifferentInstances` |
| abrupt stop before or after a commit | see below | single commit | single commit; outbox lease | `TestKilledInstanceDuringSQSBurst`, `TestGracefulStopUnderLoad`, `TestCrashAfterCommitBeforeDelete`, `TestShutdownFinishesInFlightAndReleasesTheRest` |
| integration event published again | not applicable (events move no money) | not applicable | stable `eventId`, lease takeover, conditional confirmation, immutable row | `TestAbandonedClaimIsTakenOverAfterTheLease`, `TestRepublicationKeepsTheEventID`, `TestPublishedEventsCannotBeReopened`; the stale holder's conditional confirmation is not tested directly |
| PostgreSQL or SQS temporarily unavailable | rollback; retry with the same key or message | rollback | outbox row stays unpublished and is retried | `TestFrozenDatabaseAnswers503`, `TestTransientFailureIsRetriedWithBackoff`, `TestUnavailableBrokerBacksOff` |

No scenario can leave a negative balance: the domain decides under the row lock and `CHECK (balance_minor >= 0)` refuses anything else; insufficient funds become stored `INSUFFICIENT_FUNDS` or `REVERSAL_INSUFFICIENT_FUNDS` rejections.

**Abrupt stop, step by step.**

| Where the process stops | Outcome |
| --- | --- |
| HTTP, before the commit | Postgres rolls back the transaction of the dropped connection; nothing partial is stored; resending with the same key processes the operation fresh |
| HTTP, after the commit and before the response | the resend with the same key is a replay of the stored result and balance |
| SQS, before the commit | the rollback leaves no inbox row; the message comes back after the 30 s visibility timeout |
| SQS, after the commit and before `DeleteMessage` | the message comes back and the inbox recognizes it as completed |
| outbox, after the claim or after publishing | the lease expires and the events are published again with the same `eventId` |
| pending-reference worker, mid-attempt | the transaction rolls back and frees the row lock; the row is still due and any instance picks it up on its next poll |

No test kills a process in the middle of an HTTP transaction; that case rests on Postgres rolling back the transaction of a dropped connection, plus the deferred `Rollback`.

**PostgreSQL or SQS unavailable, by surface.**

| Surface | PostgreSQL down or frozen | SQS or SNS down |
| --- | --- | --- |
| HTTP writes and reads | `503 TEMPORARILY_UNAVAILABLE` with `Retry-After: 1` within `HTTP_REQUEST_TIMEOUT`; nothing stored | not involved |
| SQS consumer | transient: message returned with visibility `min(2^receiveCount, 30)` s, DLQ after 5 receives | `ReceiveMessage` backs off from 1 s up to 10 s; reprovisions on `QueueDoesNotExist`; a failed delete is redelivered and deduplicated; a failed visibility change waits for the timeout |
| outbox publisher | claim fails, logged, backoff from the poll interval up to 5 s | events rescheduled with backoff 1 s to 5 min plus jitter, `last_error` stored |
| pending-reference worker | cycle fails, logged, retried every `PENDING_POLL_INTERVAL`; the operation stays `PENDING_REFERENCE` | not involved |
| readiness | `503`, `postgres: DOWN` | `503`, `sqs: DOWN` |
| startup | the ping in `OnStart` fails the start | queue or topic resolution fails the start |

## Authentication

**IdP: Keycloak**, provisioned in Compose from `deploy/keycloak/wallet-realm.json`, with the clients and roles already created.
It is the IdP recommended by the challenge, implements OAuth 2.0 and OIDC in a standard way and lets the whole realm be versioned as JSON, which makes the environment reproducible.

**Provisioning.**
Keycloak 26.8.1 runs `start-dev --import-realm`, with `deploy/keycloak` mounted read-only at `/opt/keycloak/data/import`, so the realm is imported when the container boots; no script or admin API call is involved.
The realm `wallet` has an access token lifetime of 300 s, the realm roles `wagering-provider` and `wallet-internal`, and the client scope `wallet-api`, which adds the audience `wallet-api` (audience mapper) and the realm roles as the multivalued `roles` claim.
Every client is confidential, with a service account and only the `client_credentials` grant; service-account users bind the roles to the clients.
The wallet instances wait for Keycloak to be healthy (`/health/ready` on management port 9000).
This is separate from `cmd/provision`, which provisions only SQS and SNS.

**Test identities.**
The secrets are development-only values committed in the realm file.

| Client | Secret | Role | `provider_id` | Purpose |
| --- | --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wagering-provider` | `provider-a` | provider flows |
| `provider-b` | `provider-b-secret` | `wagering-provider` | `provider-b` | provider isolation |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wagering-provider` | `provider-a` | 2 s tokens, for the expired-token test |
| `wallet-internal` | `wallet-internal-secret` | `wallet-internal` | none | wallet operations and reads |
| `no-role-client` | `no-role-client-secret` | none | none | valid token with no role (`403`) |

**Running the authenticated flows.**
Request a token from `http://localhost:8081/realms/wallet/protocol/openid-connect/token` and send it as `Authorization: Bearer <token>`:

```sh
token() { curl -s localhost:8081/realms/wallet/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret | jq -r .access_token; }
curl -s localhost:8091/wallets -H "Authorization: Bearer $(token wallet-internal)" \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"'"$(uuidgen)"'","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Use `wallet-internal` for `POST /wallets`, the wallet reads and reconciliation, and `provider-a` for `POST /wagering/transactions` and the provider reads; README.md has a full sequence of example calls.

**Flow.**
Service-to-service communication with `client_credentials`: each provider and the internal service have a confidential client and exchange `client_id` and secret for a JWT access token.

**Validation.**
The token is validated locally, with no call to Keycloak per request.
The parser is pinned to RS256 (`alg=none` and HS256 algorithm confusion are refused), requires `exp`, checks `nbf` when present and `iat`, `iss` equal to `OIDC_ISSUER_URL` and `aud` containing `OIDC_AUDIENCE` (default `wallet-api`).
The leeway on `exp`, `nbf` and `iat` is `OIDC_CLOCK_SKEW` (default 30 s), to absorb clock drift between Keycloak and the instances.
The signature is checked against the JWKS key named by the token's `kid`; only RSA keys with `use` empty or `sig` are loaded.
Validating locally takes the IdP out of the critical path of every operation.
The cost is that a revoked token stays valid until it expires; that is why the token lifetime is 5 minutes.

**Key cache and rotation.**
The keys are cached in memory.
An unknown `kid` triggers a reload, at most once every 10 s, so a flood of forged tokens cannot turn into a flood of requests to the IdP; a token signed with a brand-new key can therefore be refused for up to 10 s after a reload.
A reload replaces the whole key set, so keys removed from the JWKS stop being accepted after the next reload; there is no periodic or TTL refresh.
The JWKS HTTP client has a 5 s timeout.
The cache is warmed in `OnStart`, but a failure there only logs a warning and does not fail the boot: SQS processing and health checks do not depend on the IdP, and the keys load on the first request.

**Issuer and network.**
Keycloak runs with a fixed hostname (`http://localhost:8081`), so the `iss` is the same for tokens requested from outside and from inside the Compose network.
The instances fetch the JWKS through the internal address (`OIDC_JWKS_URL`) and validate the `iss` against the public address (`OIDC_ISSUER_URL`).

**Out of scope:** password registration, issuing our own tokens and flows with a human user.

## Authorization

The token identity decides the provider.
The `provider_id` claim is fixed per client in Keycloak (hardcoded claim mapper), so the provider does not choose who it is.
Roles come from the `roles` claim.

**Roles rather than scopes.**
The callers are machine clients with fixed responsibilities, not users granting delegated consent.
A realm role assigned to each client's service account maps one to one to who the caller is, an external provider or the internal back-office, is decided by Keycloak and cannot be requested by the client.
Scopes would let a client ask for a subset in the token request, which adds nothing here.
The role is a coarse class; tenant isolation comes from the fixed `provider_id` claim, a second, per-provider check that roles alone cannot express.

| Endpoint | Role | Additional rule |
| --- | --- | --- |
| `POST /wagering/transactions` | `wagering-provider` | body `providerId` equal to the token's `provider_id`, otherwise `403` |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | `wagering-provider` | `:providerId` equal to the token's, otherwise `403` |
| `GET /wagering/transactions/:transactionId` | `wagering-provider` or `wallet-internal` | a provider only sees its own; another provider's answers `404` |
| `POST /wallets`, `GET /wallets/:id`, `GET /wallets/:id/ledger`, `POST /wallets/:id/reconciliation` | `wallet-internal` | wallet operations are restricted to the internal service |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | public | no financial data |

A request with no token or an invalid one gets `401 UNAUTHENTICATED` before the role check; a valid token with no allowed role gets `403 FORBIDDEN` (`no-role-client` exists to test this).
Replays go through the same authorization: the idempotency key is unique per provider, so a provider cannot read another provider's result by reusing its key.
Authorization happens before any financial read or write; a refused request does not open a transaction.
The `404` for another provider's transaction avoids confirming that the identifier exists.

## Composition with Fx

Each area is an `fx.Module` with `fx.Provide` of its constructors; `fx.Invoke` appears only where a component must exist without anyone depending on it (HTTP server, consumer, workers).
Routes are contributed through a value group (`group:"routes"`), so the HTTP server does not import the handlers.
`internal/app/app_integration_test.go` builds the same graph with `app.Options(fx.Replace(cfg))` against real Postgres, Keycloak and MiniStack, exercises HTTP, SQS and the outbox, stops the graph and checks with `goleak` that no worker, pool or server goroutine survives.

**Composition.**
`cmd/wallet/main.go` runs `fx.New(app.Options(), fx.StartTimeout(30s), fx.StopTimeout(SHUTDOWN_TIMEOUT + 5s)).Run()`.
`app.Options` applies `fx.WithLogger(logging.FxLogger)`, so Fx's own events go to the JSON logger with `component=fx`, and lists the modules in this order:

| Module | Provides (constructor injection) | Invoke |
| --- | --- | --- |
| `config` | `config.Load() (Config, error)` | |
| `logging` | `New(Config) *slog.Logger` | |
| `metrics` | `New() *Metrics`, the `/metrics` route | |
| `postgres` | `NewPool(Lifecycle, Config, *slog.Logger) (*pgxpool.Pool, error)` | |
| `awsclient` | `aws.Config`, `*sqs.Client`, `*sns.Client`, `NewQueues(Lifecycle, Config, *sqs.Client, *sns.Client, *slog.Logger) *Queues` | |
| `auth` | `*JWKS`, `*Verifier` | |
| `wagering` (in `app.go`) | `wagering.UnitOfWork` (= `pgstore.NewUnitOfWork(pool)`), `wagering.Recorder` (= `*metrics.Metrics`), `*wagering.Service` with the `RetryPolicy` built from config | |
| `outbox` | `Register(...) *Publisher` | `*Publisher` |
| `pending` | `Register(...) *Worker` | `*Worker` |
| `sqsconsumer` | `Register(...) *Consumer` | `*Consumer` |
| `health` | `NewChecker(...)`, the live and ready routes | |
| `httpapi` | `New(Config, *wagering.Service, *auth.Verifier, *Metrics, *slog.Logger) *API`, its routes with the tag `group:"routes,flatten"` | |
| `httpserver` | `New(Params) (*http.Server, error)`, where `Params` is an `fx.In` struct consuming `group:"routes"`; `*Draining` | `*http.Server` |

Repositories are not Fx nodes: the unit of work builds them bound to each transaction.
`httpserver.AsRoute` annotates a route constructor into the group.
Each worker's `Register` constructor calls `worker.Start(lc, log, name, n, loop)`, which appends the lifecycle hook.
No file under `internal/domain` or `internal/app/wagering` imports Fx.

**Startup.**
Configuration is read and validated in the `config.Load` constructor, before any `OnStart` runs; validation errors are joined with `errors.Join`, so every bad variable is reported at once.
`OnStart` hooks then run in the order their constructors execute, which the `fx.Invoke` order (outbox, pending, sqsconsumer, httpserver) drives, within `fx.StartTimeout` (30 s):

- the pool pings Postgres (`postgres: ping: ...` fails the start);
- the AWS client, with `AWS_AUTO_PROVISION=true`, provisions first, then resolves the queue URLs (`GetQueueUrl`) and the topic ARN (idempotent `CreateTopic`), and fails the start on any error;
- the three workers start (`outbox-publisher`, `pending-reference`, `sqs-consumer`), only after the pool and the AWS resources are ready;
- the JWKS warm-up is the deliberate exception: it only logs a warning, because the IdP is needed only by HTTP requests and keys load on the first request; only the presence and format of `OIDC_ISSUER_URL` and `OIDC_JWKS_URL` are checked at start;
- the HTTP listener opens last, so no traffic is accepted before the database and SQS checks pass.

Migrations are not part of the service startup: the Compose `migrate` service runs them first.
`main` also calls `config.Load` once, only to size the stop timeout.

**Worker lifecycle.**
`worker.Start` creates a root context with `context.WithCancel(context.Background())` and a `sync.WaitGroup`.
`OnStart` launches `n` goroutines and logs `worker started` with the name and goroutine count; the workers are `sqs-consumer` (`SQS_WORKERS`, default 4), `pending-reference` (1) and `outbox-publisher` (1).
A panic is recovered, logged with the stack trace, and the loop restarts after 1 s, so that a bug does not silently stop the publisher or the consumer.
`worker.Sleep` returns at once on cancellation, so poll and backoff waits end immediately.
`OnStop` cancels the context and waits on a `done` channel closed after `wg.Wait()`, bounded by the Fx stop context: it logs `worker stopped`, or `worker did not stop in time` and returns an error.

**Shutdown.**
Fx runs the `OnStop` hooks in the reverse order of the `OnStart` hooks, and a component only starts after its dependencies; so whoever uses the pool stops before the pool closes.
The module order in `app.Options` is deliberate: `httpserver` is last, so it starts last and stops first.
The sequence on `SIGTERM` is:

1. readiness starts answering `503` (`DRAINING`), so the load balancer stops sending traffic;
2. the HTTP server stops accepting connections and waits for in-flight requests (`http.Server.Shutdown`);
3. the SQS consumer stops fetching messages, finishes the message in progress (10 s deadline, detached from shutdown) and returns the rest of the batch with visibility zero;
4. the pending-reference worker's cycle in progress is cancelled (its 30 s cycle context derives from the worker context); its row lock is released by the rollback and the operation stays due for another cycle or instance;
5. the outbox publisher finishes the batch it claimed (work detached from shutdown, bounded by `lease / 2`, 15 s); whatever it cannot finish is taken over when the lease expires;
6. the Postgres pool closes, bounded by the Fx stop context; if the context ends first it logs `postgres pool did not close in time; exiting anyway` and returns no error.

The AWS clients and the JWKS cache have no `OnStop`; nothing is closed for them.
Every `OnStop` hook shares one deadline, the Fx stop timeout: `SHUTDOWN_TIMEOUT + 5 s`, 30 s by default (also the fallback when the configuration fails to load), below the Compose `stop_grace_period` (40 s).
`SHUTDOWN_TIMEOUT` (default 25 s) is used only to compute that value.
An `OnStop` error makes the process exit with a non-zero code.
With the database frozen, the process still exits within the `stop_grace_period`, with exit code 0.
The `SIGTERM` logs show the sequence: `http server stopped`, `worker stopped` (consumer, references, outbox) and `postgres pool closed`, within a few milliseconds when there is no work in flight.
There is no wait between marking `DRAINING` and closing the listener; behind a real load balancer, a delay of a few seconds there would give it time to take the instance out of the pool.

## Context, cancellation and deadlines

Every I/O operation receives a `context.Context` and honors cancellation and deadlines.

**Where contexts come from.**
- HTTP: each authenticated route runs under `context.WithTimeout(request context, HTTP_REQUEST_TIMEOUT)` (10 s), which carries the principal and the correlation id.
- Workers: each has a root context cancelled in `OnStop`.
- Lifecycle hooks: `OnStart` runs under the Fx start context (30 s) and `OnStop` under the Fx stop context (30 s).

**Deliberate detachment.**
Work that must finish once started runs on `context.WithoutCancel` plus its own timeout: the SQS message (`SQS_MESSAGE_TIMEOUT`, 10 s), the outbox batch (`OUTBOX_LEASE / 2`, 15 s) and the transaction rollback.
The SQS acknowledgement calls (delete, visibility change, DLQ send) use a fresh `context.Background()` with a 5 s timeout each.

**Every deadline.**

| Deadline | Value |
| --- | --- |
| HTTP request | `HTTP_REQUEST_TIMEOUT`, 10 s |
| `http.Server` | `ReadHeaderTimeout` 5 s, `ReadTimeout` 10 s, `WriteTimeout` 30 s, `IdleTimeout` 60 s |
| row lock wait | `DB_LOCK_TIMEOUT`, 3 s (`lock_timeout` session parameter) |
| Postgres connect | `DB_CONNECT_TIMEOUT`, 5 s |
| SQS message | `SQS_MESSAGE_TIMEOUT`, 10 s, below the 30 s visibility timeout |
| SQS delete, visibility change, DLQ send | 5 s each |
| outbox cycle | `OUTBOX_LEASE / 2`, 15 s |
| pending-reference cycle | 30 s |
| readiness checks | 2 s each |
| JWKS fetch | 5 s client timeout |
| Fx start / stop | 30 s / `SHUTDOWN_TIMEOUT + 5 s` |

No `statement_timeout` is configured; Postgres I/O is bounded by the caller's context deadline plus `lock_timeout`, and `57014` is classified as transient in case it occurs.

## Observability

**Logs.**
JSON on stdout through `log/slog` (`slog.JSONHandler`), at `LOG_LEVEL` (`debug`, `info`, `warn` or `error`; default `info`).
Every record carries `service=wallet` and `instanceId`.
The handler does not read context values: only the correlation id travels in `context.Context`, and the other identifiers are passed as explicit attributes at each call.
The identifiers come from:

- HTTP: the `X-Correlation-Id` header is accepted (or a UUID is generated when it is missing or over 128 characters), echoed in the response and stored in the request context; the access log `http request` has `route`, `status`, `durationMs`, `correlationId`, `clientId` and `providerId`, and `wager operation` adds `transactionId`, `walletId`, `kind`, `status`, `failureCode` and `replay`;
- SQS: the envelope `messageId` becomes the correlation id; `wager message handled` has `messageId`, `transactionId`, `walletId`, `providerId`, `kind`, `status`, `failureCode` and `replay`; the retry, DLQ, delete and visibility logs carry `sqsMessageId` (the broker id) and `group` instead;
- the pending-reference worker logs `transactionId`, `walletId`, `providerId` and the stored `correlationId`; the outbox logs `eventId` and `eventType`.

The correlation id is also persisted on the transaction and copied into the event envelope.
Tokens, secrets, the `Authorization` header and full financial payloads are not logged.

**Metrics** (Prometheus, at `/metrics`, plus the Go and process collectors).

| Metric | What it measures |
| --- | --- |
| `wallet_operations_total{kind,status,channel}` | outcome by kind, status (transaction status, or `CONFLICT`, `TRANSIENT`, `FAILED`, `INVALID`) and channel (`http`, `sqs`, `worker`) |
| `wallet_idempotent_replays_total{channel}` | recognized duplicates |
| `wallet_idempotency_conflicts_total{channel}` | key or `messageId` reused with different content, or external id reused with another key |
| `wallet_lock_conflicts_total` | `lock_timeout`, deadlock, version conflict and concurrent duplicate that exhausted its attempts |
| `wallet_operation_duration_seconds{channel}` | processing latency (buckets from 1 ms to 10 s) |
| `wallet_sqs_retries_total`, `wallet_sqs_dlq_total{reason}` | retries and messages the consumer itself sent to the DLQ (the broker redrive after 5 receives is not counted) |
| `wallet_pending_reference_attempts_total{outcome}` | reference worker attempts: `processed`, `rejected`, `rescheduled`, `failed`, `error` |
| `wallet_outbox_lag_seconds` | age of the oldest unpublished event, refreshed at most every 2 s |
| `wallet_outbox_publish_total{result}` | `published` and `failed` |
| `wallet_reconciliation_divergences_total` | divergences found |
| `wallet_sqs_messages_total{result}` | handled messages: `done`, `retry`, `permanent` |
| `wallet_outbox_backlog` | events not yet published |
| `wallet_http_requests_total{route,code}`, `wallet_http_request_duration_seconds{route}` | HTTP traffic and latency per route |

`wallet_reconciliation_divergences_total`, `wallet_lock_conflicts_total` and `wallet_sqs_retries_total` are created at 0, so alerts read zero instead of no data.

**Health checks.**
`/health/live` answers `{"status":"UP"}` while the process is running.
`/health/ready` checks Postgres (ping) and SQS (`GetQueueAttributes` on the main queue) in parallel, each with its own 2 s deadline, so that a stuck dependency does not make the other one look down.
It answers `200 {"status":"UP","checks":{"postgres":"UP","sqs":"UP"}}`, or `503 {"status":"DOWN","checks":{...}}` when either check is `DOWN` or during shutdown, with `"process":"DRAINING"` added; it does not check the IdP and sends no `Retry-After`.

**Tracing and dashboards.**
Tracing with OpenTelemetry was not implemented.
A provisioned Grafana dashboard ("Wallet load") with Prometheus and a Postgres exporter ships only with the load-test profile (`loadtest/compose.load.yml`), not with the base `docker-compose.yml`; there are no production dashboards or alerts.

## Interpretations adopted

- `OPENING` has no external metadata; its events use `walletId` as the `aggregateId` and the transaction id as the `correlationId`.
- Opening a wallet with a zero balance creates only the wallet, with no `OPENING`, ledger entry or financial event.
- A `WIN` referencing a `BET` not yet received enters `PENDING_REFERENCE`, through the same mechanism as reversals.
- A `WIN` does not count as a compensation: a `BET` that produced a `WIN` still accepts a `REFUND` or a `ROLLBACK`, and the decision to request it belongs to the provider.
- `WALLET_NOT_FOUND` is not stored, because the record requires an existing wallet; a resend is evaluated again, and the result changes if the wallet comes to exist.
- `/metrics` is public to keep the local environment simple; in production it would sit on an internal port.
- `POST /wallets` is not idempotent by key: repeating the opening for the same player and currency answers `409 WALLET_ALREADY_EXISTS`, which is the challenge's contract.
- The internal service (`wallet-internal`) reads any transaction, but does not send wager operations: they require a provider identity.
- A provider does not own wallets: the challenge does not define that link, so any provider may send operations to any wallet, and the rule is that `playerId` and `walletId` must agree. An attempt with the wrong player becomes a stored `REJECTED / PLAYER_WALLET_MISMATCH` (auditable), without revealing the balance.
- An operation cannot reference itself (`referenceExternalTransactionId` equal to its own external id): `400`.
- A `WIN` on an already refunded `BET` is accepted; the credit is the provider's decision.
- The DLQ receives the `reason` attribute when the consumer uses it directly (permanent error); messages moved by the SQS redrive after 5 receives arrive without it, because the broker is the one moving them.
- The emulator loses the queues when it restarts; with `AWS_AUTO_PROVISION=true` (enabled in Compose) each instance recreates the queues, the topic and the audit subscription on start, and the consumer does the same on receiving `QueueDoesNotExist`.

## Limitations and unfinished work

- Single-entry ledger; double-entry bookkeeping was not implemented.
- Tracing with OpenTelemetry was not done; the `correlationId` flows through logs, transactions and events.
- Dashboards exist only in the load-test profile; there are no production dashboards or alerts.
- The AWS emulator does not enforce IAM: the broker policy cannot be demonstrated locally, and the sender↔provider binding (`SQS_PROVIDER_SENDERS`) is only exercised by a unit test.
- No broker access policy is defined or versioned: no SQS queue policy, no IAM principals and no policy JSON separating producers from the consumer; locally, producers and consumer share one dummy credential pair (see [SQS consumer](#sqs-consumer)).
- The proof that unauthorized access has no financial effect is partial: `TestAuthorizationAndProviderIsolation` asserts only that the `wager_transactions` row count is unchanged, not ledger rows or balances, and it does not inspect response bodies for data exposure (that relies on the status codes).
- The publishing order between events of the same wallet is not guaranteed, even with one publisher (the chunks of a claim are published in parallel, and a failed event is retried later); the consumer orders by `walletVersion`.
- An outbox event that keeps failing is retried forever at the 5-minute cap; there is no poison handling.
- The SQS consumer has no visibility heartbeat: in a full batch of slow messages, the last ones may become visible again before they start; the inbox keeps that correct, but the receive count grows.
- No `statement_timeout` is configured; statements are bounded by the request or cycle deadline and by `lock_timeout`.
- The package import boundary is a convention, not enforced by a linter.
- No test kills a process in the middle of an HTTP transaction; that case relies on Postgres rolling back the transaction of a dropped connection.
- Inbox and outbox rows are kept forever; there is no retention job.
- Load numbers come from a single host with MiniStack, not from AWS; they serve to compare versions and find bottlenecks, not to size production.

## Running from a clean checkout

**Prerequisites.**
Docker with Compose v2; Go 1.26.10 for the tests and for running commands outside the containers; `curl`, `jq` and `uuidgen` only for the example calls.
README.md has the same list and the example calls.

**Start.**
`docker compose up --build`.
Compose starts, in this order: Postgres 17.11 (roles and the `wallet` database from `deploy/postgres/init.sql`), Keycloak 26.8.1 (realm import) and MiniStack 1.5.30; then `migrate` (`/app/migrate up` as `wallet_owner`) and `provision` (queues, topic, audit subscription); then `wallet-1`, `wallet-2` and `wallet-3`.

| Service | Host port |
| --- | --- |
| Postgres | `${POSTGRES_HOST_PORT:-55432}` |
| MiniStack (SQS/SNS) | 4566 |
| Keycloak | 8081 |
| `wallet-1`, `wallet-2`, `wallet-3` | 8091, 8092, 8093 |

The environment is ready when `curl localhost:8091/health/ready` (and 8092, 8093) answers `{"status":"UP","checks":{"postgres":"UP","sqs":"UP"}}`.
`docker compose down -v` removes the `pgdata` volume and resets everything.

**Configuration.**
`.env.example` is committed, holds local example values only (the local Postgres role passwords, dummy AWS credentials accepted by MiniStack, the local Keycloak URLs) and no real secret.
Compose always loads it and then an optional, git-ignored `.env`, and overrides the hostnames that differ inside the network (`DATABASE_URL`, `AWS_ENDPOINT_URL`, `OIDC_JWKS_URL`) and `AWS_AUTO_PROVISION=true`; a clean checkout needs no copy step.
The tests and `loadtest/run.sh` read `.env` without overriding variables already exported.
`internal/config/config.go` is the complete list of service variables with their defaults and validation; `.env.example` omits four optional ones: `INSTANCE_ID` (default the hostname; Compose sets it), `DB_CONNECT_TIMEOUT` (5 s), `OIDC_CLOCK_SKEW` (30 s) and `SQS_PROVIDER_SENDERS` (empty).
It also omits `TEST_AWS_ENDPOINT_URL`, which is not a service variable: only the test helper `internal/testsupport/awstest` reads it.
`DATABASE_URL`, `OIDC_ISSUER_URL` and `OIDC_JWKS_URL` are required; an invalid value stops the service at start, with every error reported together.

## Testing

**Strategy.**
Three tiers, separated by build tags, and no infrastructure is mocked:

| Tier | Build tag | Command | Needs |
| --- | --- | --- | --- |
| unit | none | `go test ./...`, `go test -race ./...`, `go vet ./...` | nothing |
| integration | `integration` | `go test -tags integration -race -p 2 ./...` | `docker compose up -d postgres keycloak ministack` |
| end to end | `e2e` | `go test -tags e2e -race -count=1 ./e2e/` | `docker compose up -d --build` |
| failure simulations | `e2e` | `E2E_DOCKER=1 go test -tags e2e -race -count=1 ./e2e/` | the same, plus `docker` on the `PATH`; it restarts, kills, stops and pauses the running containers |

The integration files (`migrations/schema_integration_test.go`, `internal/app/app_integration_test.go`, `internal/app/wagering/service_integration_test.go`, `internal/adapters/sqsconsumer/consumer_integration_test.go`, `internal/adapters/outbox/publisher_integration_test.go`) start with `//go:build integration`, and `e2e/e2e_test.go` with `//go:build e2e`, so the plain `go test ./...` runs only unit tests.
`go test -short ./cmd/loadgen/...` skips the roughly 10 s end-to-end run of the load generator.

**Preparing the test dependencies.**
- Postgres: `pgtest` connects as superuser with `TEST_ADMIN_DATABASE_URL` (default `postgres://postgres:postgres@localhost:<POSTGRES_HOST_PORT|55432>/postgres`), creates a throwaway database `wallet_test_<uuid>` owned by `wallet_owner`, applies the embedded migrations, opens an owner pool and an application-role pool, and drops the database `WITH (FORCE)` on cleanup.
- SQS and SNS: `awstest` uses `TEST_AWS_ENDPOINT_URL` (default `http://localhost:4566`) with static credentials, and provisions per-test queues and a topic with a random suffix through the production `Provision` function, plus a raw-delivery subscription to observe published events; they are deleted on cleanup.
- Keycloak: the integration tests request real `client_credentials` tokens from `TEST_OIDC_ISSUER_URL` (default `http://localhost:8081/realms/wallet`).
- End to end: `E2E_INSTANCES`, `E2E_TOKEN_URL`, `E2E_QUEUE_URL`, `E2E_AWS_ENDPOINT_URL` and `E2E_DATABASE_URL` override the defaults (the three instances, Keycloak, MiniStack and the Compose Postgres as `wallet_owner`); the suite first checks `/health/ready` on each instance.

A missing dependency fails the test with a hint (`is docker compose up -d postgres running?`); it does not skip.
The only skips are the container tests without `E2E_DOCKER=1` and the long load-generator test under `-short`.
Isolation per test (database and queues) is why `-p 2` is safe.

**Where test doubles are used.**
Only for pure logic: `auth_test.go` serves a JWKS from `httptest` to test signatures and claims (`TestValidToken`, `TestRejectedTokens`, `TestUnknownKidRefreshIsRateLimited`); `consumer_test.go` tests the sender binding and envelope validation without SQS; the wagering integration tests swap only the metrics recorder and shorten the retry policy.
There is no mock library and no testcontainers.

**Mandatory concurrency and recovery scenarios.**

| Scenario | Test |
| --- | --- |
| the same bet 50 times | `TestSameBetFiftyTimesInParallel` (one wallet, 50 goroutines alternating HTTP and SQS); `TestSameBetFiftyTimesAcrossInstances` (50 HTTP copies over three instances plus 10 SQS copies) |
| 100.00 against two 80.00 bets | `TestTwoBetsOfEightyOnOneHundred`; `TestTwoBetsOfEightyOnDifferentInstances` |
| distinct wallets in parallel | `TestDistinctWalletsProgressInParallel` (one wallet locked, 20 others progress within 5 s); `TestDistinctWalletsAcrossInstances` (30 wallets × 5 bets) |
| three or more instances | the whole `e2e` suite, against `wallet-1`, `wallet-2`, `wallet-3` |
| consumer crash after commit | `TestCrashAfterCommitBeforeDelete`; `TestKilledInstanceDuringSQSBurst` (`SIGKILL` during 200 SQS messages, then restart: every inbox row completed, 20 debits per wallet, balance 80.00) |
| two publishers | `TestConcurrentPublishers`; `TestAbandonedClaimIsTakenOverAfterTheLease`; `TestRepublicationKeepsTheEventID` |
| reversal before reference | `TestReversalBeforeReferenceIsResolvedByWorker`; `TestPendingReferenceExpires` |
| restart preservation, pending resume included | `TestRestartPreservesIdempotencyAndPendingReferences` (`docker compose restart` of all three instances; a plain `PENDING` is never persisted, so the resumed state is `PENDING_REFERENCE`) |
| graceful stop under load | `TestGracefulStopUnderLoad` (`docker compose stop -t 40 wallet-1` during 60 bets, failover on `0`/`503`, every bet ends `200`) |
| frozen database | `TestFrozenDatabaseAnswers503` (`docker compose pause postgres`) |

**Final consistency check.**
Every scenario ends by checking the stored balance against the ledger's credits minus debits: the integration tests call `Reconcile` and fail unless `consistent`, the e2e tests call `POST /wallets/:id/reconciliation` on one of the instances, and the load test runs SQL invariants, `balance_equals_ledger` included, over the whole database plus reconciliation on a sample of wallets.
The scenarios that cross HTTP and SQS for the same operation are `TestHTTPAndSQSRaceForTheSameOperation`, `TestInboxAcrossChannels`, `TestSameBetFiftyTimesInParallel` and `TestSameBetFiftyTimesAcrossInstances` (see [Idempotency](#idempotency)).

**Duplicate tests exercise the application's dedup.**
The queues use `ContentBasedDeduplication=false`, and each test chooses the SQS `MessageDeduplicationId`, so the broker does not hide the copies:

- `TestRedeliveryIsDeduplicatedByTheInbox` sends the identical envelope twice with different dedup ids; `wallet_sqs_messages_total{result="done"}` reaching 2 proves both receipts, and the test asserts one debit and one completed inbox row;
- `TestCrashAfterCommitBeforeDelete` forces a real broker redelivery of the same SQS message (visibility set to zero) and asserts one debit;
- the e2e SQS copies use distinct `messageId`s with the same idempotency key, so the database idempotency is what catches them; 10 completed inbox rows prove the receipts.

The load-test `duplicates` profile also sends two SQS copies that reuse the `messageId`; the broker drops those, so only its two distinct-id copies reach the application.

**Authentication tests.**
`TestAuthentication` runs against the real application and Keycloak: no token, a non-JWT, a tampered payload, a stripped signature and an expired token (2 s client, then a 3 s sleep) get `401`; a valid token with no role gets `403`.
`TestRejectedTokens` covers expired, missing `exp`, future `nbf`, wrong `iss`, wrong `aud`, unknown `kid`, garbage, truncated signature, a foreign key claiming the IdP's `kid`, `alg=none` and HS256 signed with the public key.
`TestAuthorizationAndProviderIsolation` checks that provider tokens get `403` on the internal-only routes, the internal token cannot submit operations, a provider cannot submit or query for another provider (`403`) or read another provider's transaction (`404`), and a reused `Idempotency-Key` from another provider creates a new operation instead of returning the other provider's result.
It asserts that the unauthorized calls left the `wager_transactions` row count unchanged; it does not compare ledger rows or balances, and data exposure is covered by the status codes, not by inspecting bodies.

**Running the failure simulations.**
The container tests above need `E2E_DOCKER=1` and act on the running environment.
The crash simulations at integration level (`TestCrashAfterCommitBeforeDelete`, `TestAbandonedClaimIsTakenOverAfterTheLease`, `TestRepublicationKeepsTheEventID`) run with the `integration` command.
Chaos under load runs with `CHAOS=1 loadtest/run.sh steady`: `loadtest/chaos.sh` sends `SIGKILL` to `wallet-2` at 30 s and starts it at 50 s, stops `wallet-3` with `SIGTERM` at 65 s and starts it at 80 s, pauses Postgres from 90 s to 98 s and MiniStack from 105 s to 115 s (paused, never restarted, because its queues live in memory), logging each action to `events.ndjson`; it passes with zero invariant violations, the outbox drained and no DLQ growth.

## Load testing

**Command.**
`loadtest/run.sh <scenario> [extra "loadgen run" flags]`, from the repository root, for example `loadtest/run.sh steady -rate 300 -duration 3m`.
Scenarios: `baseline`, `smoke`, `steady`, `hot-wallet`, `duplicates`, `reversal-first` and `ramp`.
The script brings the stack up with `docker-compose.yml` plus `loadtest/compose.load.yml` (profile `load`), waits for readiness, seeds fresh wallets, runs the load (and `loadtest/chaos.sh` with `CHAOS=1`), drains and verifies, then writes the report.
Other switches: `LIMITS=1` (resource limits), `LOADGEN_CPUS`, `SEED_FLAGS`, `VERIFY_FLAGS`, `NO_UP=1`; `loadtest/reset.sh --yes` resets the database and queues, which is optional.
Results go to `loadtest/results/<UTC>-<scenario>/` (`report.md`, `summary.json`, the journal); the exit code is 0 for `PASS`, 1 for `FAIL` or `INVALID`.
`loadtest/README.md` has the full guide; `loadtest/results/samples/steady/report.md` is a published sample.

**Environment.**
- Topology: the three wallet instances, Postgres 17.11, MiniStack 1.5.30 and Keycloak 26.8.1, plus Prometheus 3.5.0, Grafana 12.1.1 (port 3300) and a Postgres exporter.
- Generator: a container on the Compose network that calls `http://wallet-N:8080` directly, capped at `LOADGEN_CPUS` (default 2) and 1 GB.
- Postgres keeps `fsync` and `synchronous_commit` on, and adds `pg_stat_statements`, `track_io_timing`, `log_lock_waits` and `deadlock_timeout=200ms`.
- With `LIMITS=1`: each instance gets 0.25 CPU and 64 MB with `GOMEMLIMIT=54MiB`, and Postgres 0.5 CPU and 512 MB; without it, every container can use the whole Docker VM.
- Every run writes `env.txt` (host, CPU, memory, Docker and Compose versions, limits, `DB_MAX_CONNS`, Postgres settings, images, git commit), printed in the report.
- The published sample ran on an Intel Core i7-13650HX (20 threads, 31 GiB), Linux 6.17, Docker Desktop 28.5.1 with an 8-CPU, 11.7 GiB VM, at commit `52c451c`.

**Methodology.**
The generator uses an open model with constant arrivals: latency is measured from the scheduled instant, so a slow server shows up in the percentiles instead of slowing the generator (no coordinated omission).
There is no target RPS for the service; each scenario has a default rate, overridable with `-rate`, kept below the MiniStack ceiling (around 60 op/s).
The warm-up is excluded from the statistics but kept in the journal and in verification.
A run is `INVALID` when the generator drops a task (`-max-in-flight`, 4000), its dispatch-lag p99 exceeds 100 ms, or tasks are abandoned after the drain; it is `FAIL` when verification fails, `PASS` otherwise.
Before the load, the generator waits for the outbox and the queue to be idle; after it, verification drains the backlog (up to 120 s) and checks the SQL invariants in one `REPEATABLE READ READ ONLY` transaction, cross-checks them against the client journal, reconciles a sample of wallets and checks the lookups and the DLQ.
`baseline` measures the emulator ceiling without the service.

**What the report contains.**
- Throughput: operations measured after the warm-up divided by the measured window (`opsPerSec`), per channel, plus seeding and outbox rates; the ramp knee is the last step with p99 below 500 ms and errors below 1%.
- Latency: p50, p95, p99, p99.9 and max per channel, kind and outcome, from a log-linear histogram with at most about 1.6% relative error; Grafana shows lower-resolution percentiles, and the reference numbers are those of `report.md`.
- Errors: an operation is an error when it has no answer, a final `5xx` or an unintended `4xx`; `409` and `422` are not errors, and transient `503`s retried with the same key up to 6 times count as retries; the "Errors and conflicts" section lists final errors, retries, `422` per failure code and the final database status counts.
- Conflicts: `409` responses (the `duplicates` profile sends probes expecting `IDEMPOTENCY_KEY_REUSED` and `DUPLICATE_EXTERNAL_TRANSACTION`), replays, and the maximum number of sessions waiting on a lock, sampled every 2 s from `pg_locks`; `hot-wallet` provokes lock contention on three wallets.
  The server counters `wallet_idempotency_conflicts_total` and `wallet_lock_conflicts_total` appear only on the dashboard, not in the report.
- Outbox lag: `published_at − occurred_at` percentiles computed in SQL over every event of the run, produced and published events per second, the largest backlog and oldest event sampled during the load, and the drain time; a backlog that does not drain fails the run.

| Scenario | Throughput (op/s) | HTTP p50 / p95 / p99 (ms) | Final errors | Conflicts | Outbox lag p99 (s) |
| --- | ---: | --- | ---: | --- | ---: |
| `smoke` | 20.5 | 3.7 / 16.9 / 24.3 | 0 | 22 × 409 (probes), 56 replays | 0.22 |
| `steady` | 30.0 | 5.3 / 11.5 / 35.8 | 0 | none | 0.24 |
| `hot-wallet` | 60.0 | 5.1 / 10.4 / 26.9 | 0 | 218 × 422 `INSUFFICIENT_FUNDS` | 0.46 |
| `duplicates` | 60.2 | 2.3 / 8.2 / 13.2 | 0 | 550 × 409 (all with the expected code), 1,429 replays | 0.21 |
| `reversal-first` | 20.0 | 5.9 / 10.0 / 13.4 | 0 | none | 0.27 |
| `ramp` | 80.0 (mean, 20 to 140) | 4.0 / 10.1 / 50.7 | 0 | none | 32.33 |

These are the reference runs of 2026-10-21 at commit `52c451c`, all `PASS`; `loadtest/README.md` has the full table, SQS latencies and CPU included.

## Eliminatory criteria

| Criterion | Mechanism | Code | Proof |
| --- | --- | --- | --- |
| Effective authentication | local JWT validation: RS256, JWKS, `iss`, `aud`, `exp` | `internal/platform/auth`, `deploy/keycloak/wallet-realm.json` | `TestAuthentication`, `TestRejectedTokens` |
| No unauthorized access | role per route, `providerId` bound to the token, `404` for another provider's transaction, sender binding over SQS | `internal/adapters/httpapi`, `internal/adapters/sqsconsumer` | `TestAuthorizationAndProviderIsolation`, `TestSenderMustMatchProvider` |
| No float | `int64` minor units, string DTOs, `BIGINT` columns | `internal/domain/money`, migrations | `TestJSONRejectsNumbersAndBadStrings`, money unit tests |
| No negative balance | domain decision under the row lock, `CHECK (balance_minor >= 0)` | `internal/domain/wallet`, `000001_init.up.sql` | `TestTwoBetsOfEightyOnOneHundred`, `TestTwoBetsOfEightyOnDifferentInstances`, `TestWalletInvariants` |
| No duplicate movement | partial unique indexes, ledger `UNIQUE (wallet_id, transaction_id)`, ledger bound to its transaction | migrations 000001 and 000003 | `TestSameBetFiftyTimesInParallel`, `TestSameBetFiftyTimesAcrossInstances` |
| Persistent idempotency | key, hash and result in `wager_transactions`, inbox table | `internal/app/wagering`, `internal/adapters/pgstore` | `TestIdempotencyContract`, `TestInboxAcrossChannels`, `TestRestartPreservesIdempotencyAndPendingReferences` |
| Multi-instance safety | row lock, version check, `SKIP LOCKED` claims, leases | `internal/adapters/pgstore`, `internal/adapters/outbox` | the `e2e` suite, `TestConcurrentPublishers` |
| No publication before commit | outbox in the operation's commit; the publisher reads committed rows only | `internal/app/wagering/service.go`, `internal/adapters/outbox` | `publisher_integration_test.go` |
| Auditable ledger | append-only by privilege and triggers, chained balances, reconciliation | migrations 000001–000004, `POST /wallets/:id/reconciliation` | `TestLedgerIsAppendOnlyAndConsistent`, `TestLedgerIsBoundToItsTransaction` |
| Real infrastructure in tests | real Postgres, Keycloak and MiniStack; no infrastructure mocks | `internal/testsupport` | every `integration` and `e2e` test |

## Implementation status

Updated on 2026-10-21.
Everything the challenge asks for is implemented and covered by tests; the limitations above are the known ones.

| Part | Where it is | Proof |
| --- | --- | --- |
| `Money` | `internal/domain/money` | unit tests |
| Domain: wallet, ledger, transaction, rules, events | `internal/domain/...` | unit tests |
| Schema, constraints, triggers, roles | `migrations/` | `migrations/schema_integration_test.go` |
| Use cases, idempotency, concurrency, pending operations, reconciliation | `internal/app/wagering`, `internal/adapters/pgstore` | `internal/app/wagering/service_integration_test.go` |
| HTTP API, JWT, authorization | `internal/adapters/httpapi`, `internal/platform/auth` | `internal/app/app_integration_test.go`, `internal/platform/auth/auth_test.go` |
| SQS consumer, inbox, DLQ | `internal/adapters/sqsconsumer` | `consumer_integration_test.go` |
| Outbox and publisher | `internal/adapters/outbox` | `publisher_integration_test.go` |
| Fx composition and lifecycle | `internal/app/app.go`, `internal/platform/worker` | `TestApplicationLifecycle` with `goleak` |
| Three instances, restart, `SIGKILL`, `SIGTERM`, paused Postgres | `docker-compose.yml` | `e2e/e2e_test.go` (`-tags e2e`, `E2E_DOCKER=1` for the container tests) |
| Metrics, JSON logs, health checks | `internal/platform/metrics`, `logging`, `internal/health` | `/metrics`, integration tests |
| Load test | `cmd/loadgen`, `loadtest/` | `loadtest/README.md`, [Load testing](#load-testing) |
