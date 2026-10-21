# Wallet: distributed wager processing in Go

Wallet service for game providers: an HTTP API and an SQS consumer that move wallet balances with the same guarantees, across several instances at once.
It also ships an optional load and stress test suite in [`loadtest/`](loadtest/README.md).

The technical decisions, the reasoning behind each one, and the limitations are in [`ARCHITECTURE.md`](ARCHITECTURE.md).
The load test environment is described in [`loadtest/README.md`](loadtest/README.md).

## At a glance

- Go 1.26, Uber Fx, `net/http`, `pgx/v5` with explicit SQL, PostgreSQL 17, Keycloak 26 (OIDC, `client_credentials`), SQS/SNS FIFO on MiniStack, Docker Compose.
- Money as `int64` cents, never `float`; input accepted only in the canonical form `"25.00"`.
- Pessimistic lock per wallet (`SELECT ... FOR UPDATE`) with a version check; no global lock.
- Persistent idempotency on `(providerId, idempotencyKey)` and `(providerId, externalTransactionId)`, with a canonical payload hash, shared by HTTP and SQS.
- Balance, ledger, inbox and outbox in the same commit; the ledger is append-only and bound to its transaction by constraints and triggers, so the database refuses inconsistent states even when they come from outside the code.
- Inbox in the SQS consumer, outbox with concurrent publishers (`SKIP LOCKED` and a lease), DLQ, and a pending-reference worker with backoff and TTL.
- Three independent instances in Compose; the concurrency and failure tests run against them.

## Prerequisites

| Tool | Version | Used for |
| --- | --- | --- |
| Docker with Compose v2 | Docker 24+ | starting the environment |
| Go | 1.26.10 (the one in `go.mod`) | running the tests and the commands outside the container |
| `curl`, `jq` and `uuidgen` (`util-linux` package on Linux; built in on macOS) | any | the examples below |

Host ports in use: `55432` (Postgres), `8081` (Keycloak), `4566` (MiniStack), `8091`, `8092` and `8093` (instances).
If `55432` is taken, create a `.env` with `POSTGRES_HOST_PORT=55433` and the Postgres URLs pointing to the new port (`DATABASE_URL`, `MIGRATIONS_DATABASE_URL`, `TEST_ADMIN_DATABASE_URL`).
Compose, `loadtest/run.sh` and `go test` read that `.env` on their own.

## Starting the environment

```sh
docker compose up --build
```

The first start takes 2 to 3 minutes, mostly pulling images and booting Keycloak.
Compose resolves the order:

1. `postgres` creates the roles (`deploy/postgres/init.sql`), `keycloak` imports the realm (`deploy/keycloak/wallet-realm.json`) and `ministack` starts the SQS/SNS emulator;
2. `migrate` applies the migrations as the schema owner and exits;
3. `provision` creates the queues, the DLQ with redrive, the events topic and the audit queue, and exits;
4. `wallet-1`, `wallet-2` and `wallet-3` start, each with its own process, memory and pool.

It is ready when all three answer:

```sh
for p in 8091 8092 8093; do curl -s localhost:$p/health/ready; echo; done
# {"status":"UP","checks":{"postgres":"UP","sqs":"UP"}}
```

To start from scratch (wipes the database): `docker compose down -v`.

## Environment variables

They are all in [`.env.example`](.env.example), with local values and no real secrets.
Compose reads that file and, if present, a `.env` on top of it; inside the Compose network it overrides the addresses that change (`postgres:5432`, `ministack:4566`, `keycloak:8080`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | — | service login (`wallet_service`, without `UPDATE`/`DELETE` on the ledger) |
| `MIGRATIONS_DATABASE_URL` | — | schema owner, used only by the `migrate` command |
| `DB_MAX_CONNS`, `DB_LOCK_TIMEOUT` | `10`, `3s` | pool per instance and maximum wait for a wallet lock |
| `OIDC_ISSUER_URL`, `OIDC_JWKS_URL`, `OIDC_AUDIENCE` | —, —, `wallet-api` | JWT validation |
| `OIDC_CLOCK_SKEW` | `30s` | clock tolerance on `exp`/`nbf` |
| `AWS_ENDPOINT_URL`, `AWS_REGION` | —, `us-east-1` | empty talks to real AWS |
| `WAGER_QUEUE_NAME`, `WAGER_DLQ_NAME`, `EVENTS_TOPIC_NAME` | `wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wallet-events.fifo` | messaging resources |
| `AWS_AUTO_PROVISION` | `false` (`true` in Compose) | recreates queues and topic on start; the emulator loses them when it restarts |
| `SQS_WORKERS`, `SQS_WAIT_TIME`, `SQS_MESSAGE_TIMEOUT` | `4`, `10s`, `10s` | consumer; the per-message deadline stays below the 30 s visibility timeout |
| `SQS_PROVIDER_SENDERS` | empty | `senderId=providerId,...`: which SQS sender may speak for which provider |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL`, `OUTBOX_LEASE` | `50`, `200ms`, `30s` | publisher |
| `PENDING_BASE_DELAY`, `PENDING_MAX_DELAY`, `PENDING_TTL` | `1s`, `60s`, `10m` | backoff and expiry of pending references |
| `HTTP_REQUEST_TIMEOUT` | `10s` | deadline of a request, database included; exceeding it returns `503` |
| `SHUTDOWN_TIMEOUT`, `LOG_LEVEL` | `25s`, `info` | shutdown and log level (`debug`, `info`, `warn`, `error`) |
| `POSTGRES_HOST_PORT` | `55432` | Postgres port on the host |
| `TEST_ADMIN_DATABASE_URL` | superuser on `localhost:55432` | integration tests create throwaway databases with it |

## Queues

The `provision` service creates everything idempotently on every `docker compose up`:

| Resource | Configuration |
| --- | --- |
| `wager-transactions.fifo` | input; visibility timeout 30 s; redrive to the DLQ after 5 receives |
| `wager-transactions-dlq.fifo` | messages that cannot be processed; 14-day retention; `reason` attribute |
| `wallet-events.fifo` (SNS) | outgoing events; `MessageGroupId = walletId`, `MessageDeduplicationId = eventId`, `eventType` attribute |
| `wallet-events-audit.fifo` | subscribes to the topic with raw delivery, for inspection |

To run it again by hand: `docker compose run --rm provision`.

## Migrations

```sh
docker compose run --rm --entrypoint /app/migrate migrate version   # current state
docker compose run --rm --entrypoint /app/migrate migrate up        # apply
docker compose run --rm --entrypoint /app/migrate migrate down 1    # roll back N steps (N is required)
```

Outside the container, with Go: `MIGRATIONS_DATABASE_URL=postgres://wallet_owner:wallet_owner@localhost:55432/wallet?sslmode=disable go run ./cmd/migrate up`.
The migrations are embedded in the binary (`migrations/*.sql`), so the command, the tests and the environment apply exactly the same files.
The `down` of the first migration deletes every financial record and only makes sense locally.

## Test identities

The realm is imported from the versioned file; no manual registration is needed.

| Client | Secret | Role | `provider_id` | Used for |
| --- | --- | --- | --- | --- |
| `wallet-internal` | `wallet-internal-secret` | `wallet-internal` | — | opening wallets, reading wallet and ledger, reconciling, reading any transaction |
| `provider-a` | `provider-a-secret` | `wagering-provider` | `provider-a` | sending and querying its own operations |
| `provider-b` | `provider-b-secret` | `wagering-provider` | `provider-b` | isolation between providers |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wagering-provider` | `provider-a` | 2 s token, to test expiry |
| `no-role-client` | `no-role-client-secret` | none | — | valid token without permission |

`provider_id` is a fixed claim per client in Keycloak: the provider does not choose who it is.

## Example calls

```sh
token() { curl -s localhost:8081/realms/wallet/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret | jq -r .access_token; }
INTERNAL=$(token wallet-internal); PROVIDER=$(token provider-a)
PLAYER=$(uuidgen | tr A-Z a-z)

# Open a wallet (creates OPENING, its ledger entry and events in the same commit)
WALLET=$(curl -s -X POST localhost:8091/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d '{"playerId":"'$PLAYER'","initialBalance":{"amount":"1000.00","currency":"BRL"}}' | tee /dev/stderr | jq -r .id)

# Bet, on another instance
bet() { curl -s -X POST localhost:$1/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: provider-a:$2" \
  -d '{"providerId":"provider-a","externalTransactionId":"'$2'","playerId":"'$PLAYER'","walletId":"'$WALLET'",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"'$3'","money":{"amount":"'$4'","currency":"BRL"}'"$5"'}'; echo; }
bet 8092 tx-1 BET 25.00            # 200 PROCESSED, balance 975.00
bet 8093 tx-1 BET 25.00            # 200, idempotentReplay: true, same balance
bet 8091 tx-1 BET 26.00            # 409 IDEMPOTENCY_KEY_REUSED

# Reversal before its reference: 202 PENDING_REFERENCE; resolved by the worker when the bet arrives
bet 8091 tx-3 REFUND 10.00 ',"referenceExternalTransactionId":"tx-2"'
bet 8092 tx-2 BET 10.00
sleep 1   # the worker polls pending operations every 500 ms
curl -s localhost:8093/providers/provider-a/wagering/transactions/tx-3 -H "Authorization: Bearer $PROVIDER" | jq .status   # "PROCESSED"

# Read, paginated ledger and reconciliation
curl -s localhost:8091/wallets/$WALLET -H "Authorization: Bearer $INTERNAL"
curl -s "localhost:8091/wallets/$WALLET/ledger?limit=2" -H "Authorization: Bearer $INTERNAL"
curl -s -X POST localhost:8091/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL"
```

Through SQS, with the AWS CLI in a container (the body follows section 10 of the challenge):

```sh
docker run --rm --network wallet-service_default -e AWS_ACCESS_KEY_ID=000000000000 -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.27.50 --endpoint-url http://ministack:4566 sqs send-message \
  --queue-url http://ministack:4566/000000000000/wager-transactions.fifo \
  --message-group-id $WALLET --message-deduplication-id msg-1 --message-body '{"messageId":"msg-1",
  "type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a",
  "externalTransactionId":"tx-1","idempotencyKey":"provider-a:tx-1","playerId":"'$PLAYER'","walletId":"'$WALLET'",
  "roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}'
```

The `wallet-service_default` network belongs to the default project name; with another `COMPOSE_PROJECT_NAME` it becomes `<name>_default`.
This message repeats `tx-1`, already sent over HTTP: the consumer recognizes it as a replay, completes the inbox and does not debit again.
Published events show up in `wallet-events-audit.fifo`; metrics are at `localhost:8091/metrics`.

## Tests

| Command | What it runs | Requires |
| --- | --- | --- |
| `go test ./...` | unit tests | nothing |
| `go test -race ./...` | unit tests with the race detector | nothing |
| `go vet ./...` | static analysis | nothing |
| `go test -tags integration -race -p 2 ./...` | integration: schema, use cases, SQS, outbox, JWT, Fx graph | `docker compose up -d postgres keycloak ministack` |
| `go test -tags e2e -race -count=1 ./e2e/` | multiple instances against `wallet-1..3` | `docker compose up -d --build` |
| `E2E_DOCKER=1 go test -tags e2e -race -count=1 ./e2e/` | adds restart, `SIGKILL`, `SIGTERM` and `docker pause` of Postgres | same, plus `docker` on the PATH; **restarts and kills the containers of the running environment** |

The integration tests use real Postgres, Keycloak and MiniStack, with no infrastructure mocks.
Each test creates its own throwaway database (`TEST_ADMIN_DATABASE_URL`) and its own randomly named queues, so they can run in parallel and do not interfere with the environment.
`-p 2` limits how many packages run in parallel, so smaller machines are not overloaded.
The tests read the root `.env` (without overriding variables already exported), so the alternative port applies to them too.

Where the mandatory scenarios are (section 13 of the challenge):

| Scenario | Test |
| --- | --- |
| Same bet 50 times in parallel, one debit | `TestSameBetFiftyTimesInParallel`, `TestSameBetFiftyTimesAcrossInstances` |
| Two bets of 80.00 on 100.00 | `TestTwoBetsOfEightyOnOneHundred`, `TestTwoBetsOfEightyOnDifferentInstances` |
| Distinct wallets in parallel | `TestDistinctWalletsProgressInParallel`, `TestDistinctWalletsAcrossInstances` |
| Three independent instances | `e2e/e2e_test.go` |
| Consumer interrupted after the commit and before the delete | `TestCrashAfterCommitBeforeDelete` |
| Two publishers competing for the outbox; publication recovery | `TestConcurrentPublishers`, `TestAbandonedClaimIsTakenOverAfterTheLease`, `TestRepublicationKeepsTheEventID` |
| Reversal before its reference: resolution and expiry | `TestReversalBeforeReferenceIsResolvedByWorker`, `TestPendingReferenceExpires` |
| Restart preserving idempotency and pending operations | `TestRestartPreservesIdempotencyAndPendingReferences` |
| HTTP and SQS for the same operation | `TestHTTPAndSQSRaceForTheSameOperation`, `TestInboxAcrossChannels` |
| Provider isolation and invalid credentials | `TestAuthorizationAndProviderIsolation`, `TestAuthentication`, `TestRejectedTokens` |
| Direct attacks on the schema | `TestLedgerIsBoundToItsTransaction`, `TestReviewRegressions` and the others in `migrations/` |
| Database unavailable | `TestFrozenDatabaseAnswers503` |

There is no asynchronous acceptance: an operation without a dependency is completed in the same commit in which it is recorded, and the only durable pending state is `PENDING_REFERENCE` (see `ARCHITECTURE.md`, "No asynchronous acceptance").

## Load test

See [`loadtest/README.md`](loadtest/README.md): reproducible command, scenarios, methodology, throughput, p50/p95/p99, errors, conflicts and outbox lag, with the invariants verified at the end of every run.

## Layout

```
cmd/wallet        process: API, consumer and workers
cmd/migrate       applies and rolls back migrations
cmd/provision     creates queues, DLQ and topic
cmd/loadgen       load generator and verifier
internal/domain   Money, wallet, ledger, transaction, events (no Fx, HTTP, SQS or pgx)
internal/app      use cases (wagering) and Fx composition (app.go)
internal/adapters pgx, HTTP, SQS, outbox, pending-reference worker
internal/platform config, pool, AWS, HTTP server, JWT, metrics, logs, workers
migrations        versioned SQL
deploy            Keycloak realm and Postgres roles
e2e               tests against the three instances
loadtest          load environment (Prometheus, Grafana, scenarios)
```
