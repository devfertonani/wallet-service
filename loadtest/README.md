# Load and stress tests

This directory and the generator in `cmd/loadgen` make up the load environment of the wallet service.
The goal is not to hit an RPS number, but to measure throughput and latency while proving that financial correctness holds under load, with three instances competing for the same wallets over HTTP and SQS.
Every run ends with an invariant check; a fast run with a broken invariant fails, whatever its throughput.

## Prerequisites

- Docker with Compose v2 (tested with Docker Desktop 28.5 and Compose 2.40 on Linux).
- Bash, for the scripts.
- Go 1.26, only to run the generator outside the container or the unit tests.
- About 3 GB of free memory for the stack with the load profile.

## Quick start

From the repository root:

```sh
loadtest/run.sh smoke
```

The script starts the stack with the load profile, waits for the three instances, Keycloak and the queues, empties the event audit queue, seeds wallets through the API, waits for the system to go idle, generates the load, waits for the outbox and the queue to drain, checks the invariants and writes the report.
During load and drain it samples the CPU of every container (`docker stats`), so the report can say which component saturated.
`smoke` runs about 30 seconds of load at 20 op/s and goes through every task type (HTTP and SQS rounds, duplicates and early reversals).
The result lands in `loadtest/results/<UTC>-smoke/report.md`, and the exit code is `0` for `PASS` and `1` for `FAIL` or `INVALID`.

Other scenarios:

```sh
loadtest/run.sh baseline                     # ceiling of MiniStack alone, without the service
loadtest/run.sh steady                       # 30 op/s for 2 min
loadtest/run.sh steady -rate 60 -duration 5m # above ~60 op/s MiniStack saturates (see Results)
loadtest/run.sh hot-wallet
loadtest/run.sh duplicates
loadtest/run.sh reversal-first
loadtest/run.sh ramp                         # 20 op/s, +20 every 30 s, up to 140
CHAOS=1 loadtest/run.sh steady               # with injected failures
```

Arguments after the scenario name go to `loadgen run`; `SEED_FLAGS` and `VERIFY_FLAGS` pass flags to seeding and verification (`SEED_FLAGS="-wallets 10000"`).
`NO_UP=1` skips `docker compose up`, when the stack is already running with the override.
`docker run --rm wallet-service-loadgen:local scenarios` (or `go run ./cmd/loadgen scenarios`) lists the scenarios with their defaults.

## Scenarios

The defaults stay below the MiniStack ceiling measured on the reference machine (section [Results](#results)), so that a `loadtest/run.sh <scenario>` passes from a clean `docker compose up`; all of them can be changed by flag.

| Scenario | What it proves | Default |
| --- | --- | --- |
| `smoke` | the environment works end to end | 50 wallets, 20 op/s, 30 s; 70% HTTP rounds, 20% SQS rounds, 5% duplicates, 5% early reversals |
| `steady` | stable throughput and latency with the realistic mix | 1,000 wallets, 30 op/s, 2 min, 20 s warm-up; 80% HTTP, 20% SQS |
| `hot-wallet` | lock contention on the same row across the three instances, with no negative balance and no lost update | 3 wallets with 100.00, 60 op/s (20 op/s per wallet), 1 min; 55% `BET` and 45% `WIN` of 1.00 to 10.00 |
| `duplicates` | each operation has a single effect, through any channel and instance | 200 wallets, 60 op/s, 1 min; each `BET` sent 10 times within 300 ms, plus 2 conflict probes |
| `reversal-first` | a reversal that arrives before its reference is resolved later | 200 wallets, 20 op/s, 1 min; `REFUND`/`ROLLBACK` first, `BET` 0 to 2 s later |
| `ramp` | where the machine's knee is, and why | 1,000 wallets; 20 op/s, +20 every 30 s, up to 140, for 3.5 min; each step also shows the outbox backlog and the MiniStack CPU |
| `baseline` | the emulator ceiling alone, with no business rules | SQS `SendMessageBatch` and `ReceiveMessage` + `DeleteMessageBatch` on a scratch queue, SNS `Publish` and `PublishBatch` on the real topic with the fan-out to the audit queue; 8 callers, 2,000 groups, 20 s per phase |

Details of each task:

- **Round** (`steady`, `smoke`, `ramp`): a `BET` of 1.00 to 20.00 and, if processed, its settlement: `WIN` (45%), `LOSS` of `0.00` (45%), `REFUND` (5%) or `ROLLBACK` (5%) of the bet.
  The whole round goes through one channel: HTTP, with the instances in round robin, or SQS, with `MessageGroupId = walletId`, sending the settlement only after the broker acknowledges the bet, to keep the order within the group.
- **Hot wallet** (`hot-wallet`): a single operation on one of a few wallets; the negative drift takes the balance to zero, so contention that ends in `INSUFFICIENT_FUNDS` also happens.
  Verification recomputes the balance and version of each wallet from the responses alone: a lost update shows up as a version lower than expected.
- **Duplicate** (`duplicates`): the same `BET` with the same `Idempotency-Key` sent 6 times over HTTP (2 on each instance), 2 times over SQS with the same `messageId` (broker and inbox deduplication) and 2 times over SQS with distinct `messageId`s (deduplication by key).
  Then two probes that must be refused: the same key with another amount (`409 IDEMPOTENCY_KEY_REUSED`) and another key with the same `externalTransactionId` (`409 DUPLICATE_EXTERNAL_TRANSACTION`).
- **Early reversal** (`reversal-first`): `REFUND` or `ROLLBACK` of a `BET` not yet sent, which must answer `202 PENDING_REFERENCE`; then the `BET`; then the generator polls the reversal until it leaves `PENDING_REFERENCE` and measures the resolution time.
  `-orphan-ratio 0.05` leaves 5% of the reversals without a reference; they only end in `REFERENCE_NOT_FOUND` after the service TTL (10 min by default), so use it together with a shorter TTL on the service.

## How the generator works

The generator is a dedicated Go binary (`cmd/loadgen`), in the same module, with no dependencies beyond those the service already uses.
Off-the-shelf tools handle constant-rate HTTP, but they do not produce to SQS FIFO, do not cross the same operation between HTTP and SQS, do not orchestrate reversals before their reference and do not check the database at the end; here all of that lives in one place.

- **Open-loop scheduler** (`internal/sched`): each task has a scheduled instant computed from the rate alone; latency is measured from it, not from the actual send.
  That way a slow server shows up in the p99 instead of slowing the generator down (coordinated omission).
  If the number of in-flight tasks exceeds the limit (`-max-in-flight`, 4,000), the arrival is dropped and the run becomes `INVALID`; the same applies to a dispatch delay p99 above 100 ms.
- **Tokens** (`internal/auth`): one `client_credentials` token per client (`provider-a`, `provider-b`, `wallet-internal`), requested before the load, cached and renewed in the background at 70% of its lifetime (300 s), with a single in-flight call per client.
  The report shows how many tokens were requested; Keycloak stays out of the measured path.
- **HTTP** (`internal/target`): keep-alive connections and round robin across `wallet-1`, `wallet-2` and `wallet-3`.
  `503`, `502`, `504`, `429`, timeouts and refused connections are retried with the same `Idempotency-Key` and the same body, with exponential backoff and jitter (50 ms to 2 s, honoring `Retry-After`), up to 6 attempts, always on the next instance; it is what a real provider would do.
- **SQS**: `SendMessageBatch` with up to 10 messages per call and 5 ms of wait to fill the batch, `MessageGroupId = walletId` and `MessageDeduplicationId = messageId`.
- **Journal** (`journal.ndjson`): one line per request, with key, wallet, kind, amount, channel, instance, scheduled instant, attempts, HTTP status, `transactionId`, balance and whether it was a replay.
  It is the oracle independent from the database during verification.
- **Audit queue consumer** (`target.Drainer`): the `wallet-events-audit.fifo` queue subscribes to the events topic and nobody consumes it; the generator empties it at the start and consumes it during load and drain, as a real consumer would, and measures the end-to-end delivery of each event (`occurredAt` to receipt).
  Without that, the queue only grows and MiniStack gets slower with every publish (see [Emulator ceiling](#emulator-ceiling)).
- **Idle start**: before the load, `loadgen run` waits for the outbox and the input queue to be empty (up to 3 min, `-idle-timeout`); seeding leaves two opening events per wallet, which must not be charged to the scenario.
- **CPU and memory per container**: `run.sh` samples `docker stats` every ~2 s into `docker_stats.csv`, with the Compose service name; the report splits the CPU mean and maximum between load and drain, and shows the memory peak against the container limit.
- **Histograms** (`internal/hist`): log-linear in microseconds (relative error up to 1.6%), one per channel, kind and outcome, with p50, p95, p99, p99.9 and maximum.

## Methodology

- Open model with a constant rate (or in steps for `ramp`); the target rate is in operations per second and the generator converts it into tasks per second using the average number of operations per task.
- Operations scheduled during warm-up are left out of the statistics, but they are in the journal and in the verification.
- In a round, the bet is measured from its scheduled instant and the settlement from the moment the provider would send it, after the bet's response.
- Client latency includes retries; server latency (`wallet_operation_duration_seconds`) is in Grafana, next to it.
- On SQS, client latency is only the send to the broker; end-to-end latency (scheduled instant to the stored transaction, `created_at`) is computed during verification.
- The generator runs in a container on the same Compose network and talks to `wallet-N:8080` directly, without going through the Docker Desktop port proxy, capped at 2 CPUs (`LOADGEN_CPUS`).
- Each run opens new wallets through the API (`POST /wallets`, the real opening path), so consecutive runs need no reset: the cross-check with the journal covers exactly the wallets of the run, and the SQL invariants cover the whole database.
- Postgres runs with `fsync` and `synchronous_commit` on; the load profile only turns on `pg_stat_statements`, `track_io_timing` and `log_lock_waits`.
  Turning durability off in a wallet test would inflate the number and invalidate the conclusion.
- Knee (`ramp`): the last step with p99 below 500 ms and errors below 1%, before the first step that violates either; the other scenarios can use fractions of it (`-rate`).

## Verification

`loadgen verify` first waits for the outbox, the inbox and the input queue to empty (up to 120 s, `-drain-timeout`) and measures the drain time.
Then it runs, in a `REPEATABLE READ READ ONLY` transaction, the invariants in [`cmd/loadgen/internal/verify/invariants.sql`](../cmd/loadgen/internal/verify/invariants.sql) over the whole database:

- stored balance equal to the ledger sum, and version equal to the last `wallet_version`;
- continuous ledger chain (`balance_before` of each entry equal to the `balance_after` of the previous one);
- no negative balance, in the wallet or at any point of the ledger;
- no duplicates by `(provider, externalTransactionId)` or by `(provider, Idempotency-Key)`;
- every `PROCESSED` transaction that moves the balance has exactly one entry, with the same amount and resulting balance; `LOSS`, `REJECTED`, `FAILED` and pending ones have none;
- entry direction consistent with the kind, at most one successful reversal per reference, with the same amount and the same wallet;
- no `FAILED` or `PENDING` transaction, no expired `PENDING_REFERENCE`;
- the whole outbox published, with one event of each type for each transaction and matching entry; the whole inbox completed.

The same file runs in `psql` (`psql "$URL" -f cmd/loadgen/internal/verify/invariants.sql`); each query lists the rows that violate the rule.
Several of these rules are also enforced by constraints and triggers in the migrations; checking after the load is the audit that they held, not a substitute.

Cross-check with the journal, for the wallets of the run:

- every `200` or `422` response exists in the database with the same status, `failureCode` and `transactionId`; every `202` response and every SQS message accepted by the broker exists in the database;
- copies of the same operation return the same `transactionId` and the same balance, and only one is processed as the original;
- the probes get exactly the expected `409`, and the stored amount is the one of the original operation;
- no transaction in the wallets of the run outside the journal;
- balance and version of each wallet recomputed from the journal alone (opening, plus credits, minus processed debits) equal to the stored ones.

Finally, it samples the API: `POST /wallets/{id}/reconciliation` on 50 random wallets and on every hot wallet (all of them must return `consistent: true`) and `GET /providers/{p}/wagering/transactions/{id}` on 50 transactions, compared with the database; and it checks that the DLQ did not grow.
Any violation fails the run and makes `verify` exit with code `1`.

## Reading the report

`report.md` starts with the verdict:

- `PASS`: invariants and cross-check passed and the generator kept its schedule;
- `INVALID`: correctness passed, but the generator saturated or fell behind; the performance numbers do not count;
- `FAIL`: some check failed; the reasons come right below, and samples of the violating rows are in the Verification section.

Then come the command to reproduce, the environment fingerprint (commit, CPU, memory, Docker and VM version, images, Postgres parameters), the scenario parameters, throughput per channel, latency per channel, kind and outcome, errors and conflicts (retries, `503`, replays, `409`, `422` by `failureCode`, uncertain outcome), outbox lag (p50, p95, p99 and maximum of `published_at − occurred_at`, computed in SQL, and drain time), the `ramp` step table, the line-by-line verification, data about the generator itself and the ten most expensive queries from `pg_stat_statements`.
`summary.json` has the same numbers for automated comparison.

Files in the result directory:

| File | Contents |
| --- | --- |
| `report.md`, `summary.json` | report and consolidated numbers |
| `command.txt`, `env.txt` | exact command and environment fingerprint |
| `wallets.json`, `seed.json` | opened wallets and seeding statistics |
| `journal.ndjson`, `run.json` | operation journal, histograms and database samples every 2 s |
| `verify.json` | result of each check |
| `docker_stats.csv` | CPU and memory of each container, every ~2 s |
| `baseline.json` | emulator ceiling, in the directory of a `run.sh baseline` |
| `events.ndjson` | chaos actions, when there are any |
| `pg_stat_statements.txt` | most expensive queries |

## Live observability

With the load profile, Grafana is at <http://localhost:3300> (anonymous read access; `admin`/`admin` to edit) and Prometheus at <http://localhost:9090>, both bound to `localhost` only.
The ports change with `LOAD_GRAFANA_PORT` and `LOAD_PROMETHEUS_PORT`.
The "Wallet load" dashboard shows server throughput by channel, status and kind, the generator's target and executed rate, server and client p50/p95/p99 latency, replays and conflicts, SQS retries and DLQ, outbox lag and backlog, pending references, and Postgres connections, locks, commits, rollbacks and deadlocks.
Prometheus scrapes every 2 s from the three instances, from `postgres-exporter` and from the generator itself (`loadgen:9100`, only while `loadgen run` is running).
The Grafana percentiles come from low-resolution Prometheus histograms; the reference numbers are the ones in `report.md`.

## Chaos

`CHAOS=1 loadtest/run.sh steady` runs `loadtest/chaos.sh` alongside the load, on a fixed timeline counted from its start:

| Instant | Action |
| --- | --- |
| 30 s | `kill wallet-2` (SIGKILL, no graceful shutdown) |
| 50 s | `start wallet-2` |
| 65 s | `stop wallet-3` (SIGTERM, graceful shutdown) |
| 80 s | `start wallet-3` |
| 90 s | `pause postgres` for 8 s |
| 105 s | `pause ministack` for 10 s |

It needs at least 2 minutes of load (the `steady` default).
The actions are recorded in `events.ndjson` and in the report.
The criterion stays the same: zero violations, outbox drained and no DLQ growth; final errors and latency during the failure are reported, not hidden.
MiniStack is paused, never restarted, because it keeps the queues in memory and a restart would lose messages in the broker.

## Limited resources

Without limits, every container can use the whole Docker VM, and MiniStack saturates long before the wallet or Postgres feel the load.
`LIMITS=1` adds `loadtest/compose.limits.yml`, which gives the three instances and Postgres a small, fixed share of CPU and memory, close to a small production pod:

```sh
LIMITS=1 loadtest/run.sh ramp
LIMITS=1 LIMIT_WALLET_CPUS=0.1 LIMIT_POSTGRES_CPUS=0.25 loadtest/run.sh ramp
```

| Variable | Default | Effect |
| --- | --- | --- |
| `LIMIT_WALLET_CPUS` | `0.25` | CPU of each instance; Go 1.25+ derives `GOMAXPROCS` from the limit |
| `LIMIT_WALLET_MEM` | `64m` | memory of each instance, without swap |
| `LIMIT_WALLET_GOMEMLIMIT` | `54MiB` | GC soft limit, ~85% of the memory, so the GC works before the kernel kills the process |
| `LIMIT_POSTGRES_CPUS` | `0.5` | Postgres CPU |
| `LIMIT_POSTGRES_MEM` | `512m` | Postgres memory, without swap; the default `shared_buffers` (128 MB) stays at the usual ~25% |

Only the components that would go to production are limited; MiniStack and Keycloak stand in for AWS and the identity provider and stay unlimited, and the generator keeps its own cap (`LOADGEN_CPUS`).
Without swap, a container that exceeds its limit is killed by the kernel, as in Kubernetes, instead of slowing down on swap.
`env.txt` records the limits Docker actually applied (not the ones in the file), and `run.sh` compares restarts and `OOMKilled` before and after the run (`containers_before.txt` and `containers_after.txt`) and warns if any container went down.
The report shows each service's memory peak against its limit; the CPU of a limited service saturates at its own limit (0.25 core = 25%), not at 100%.
The result directory gets the `-limits` suffix.
To go back to unlimited containers, a plain `loadtest/run.sh` without `LIMITS` recreates the ones that changed; `LIMITS=1 loadtest/reset.sh --yes` keeps the limits.

Runs from 2026-10-21 on the reference machine, commit `25deab5` plus these changes, database with ~500 MB from previous runs:

| Run | Verdict | `ramp` knee | Outbox lag p99 | Postgres max CPU | Max memory per instance | What limited it |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| no limits | `PASS` | 100 op/s | 25 s | 91% | 25 MiB | MiniStack (98%) |
| `LIMITS=1` (0.25 CPU and 64 MiB per instance; 0.5 CPU and 512 MiB for Postgres) | `PASS` | 100 op/s | 96 s | 55% (limit 50%) | 24 / 64 MiB | MiniStack (110% from 120 op/s on); Postgres at its limit in the last steps and during drain |
| `LIMITS=1`, 0.1 CPU per instance and 0.25 for Postgres | `FAIL` | none (p99 4.3 s already at 20 op/s) | outbox did not drain in 2 min (9,140 events) | 34% (limit 25%) | 60 / 64 MiB | the instances, at their CPU limit from the first step |
| `LIMITS=1` with `LIMIT_POSTGRES_MEM=256m`, `smoke` | `FAIL` | — | — | — | 18 / 64 MiB | a Postgres backend killed for lack of memory during the invariants |

What this shows:

- With 0.25 core and 64 MiB per instance, the wallet keeps up with the MiniStack ceiling: the knee does not move, each instance uses about 6% CPU and 24 MiB, and the one that hits its limit is Postgres, which quadruples the outbox lag during drain.
- With 0.1 core, the instances saturate already at 20 op/s: HTTP latency goes past seconds, the generator makes 28 thousand retries and the heap touches `GOMEMLIMIT` (60 of 64 MiB), without getting killed.
  The ledger stays intact (the SQL invariants pass, except for the undrained outbox), but 2 SQS messages (a `REFUND` and a `WIN`) went to the DLQ without being applied: each attempt exceeds the 10 s `SQS_MESSAGE_TIMEOUT`, the consumer returns the message with waits of 2, 4, 8 and 16 s, and the redrive policy (`MaxReceiveCount = 5`) moves it to the DLQ after about 80 s of overload.
  Sustained overload turns into manual DLQ redrive work.
- With 256 MiB, the load fits in Postgres (about 140 MiB), but the whole-database invariant queries of `loadgen verify` make the kernel kill a backend, and the server goes through crash recovery; with `shared_buffers` at 50% of the memory there is no room left for heavy queries.

## Reset

There is no need to reset between runs.
To start from an empty database:

```sh
loadtest/reset.sh --yes
```

The ledger is append-only even for the schema owner (triggers refuse `UPDATE`, `DELETE` and `TRUNCATE`), so the reset stops the instances, recreates the `wallet` database with the same roles and privileges as `deploy/postgres/init.sql`, applies the migrations, empties the queues and starts the instances again.
To wipe everything, Keycloak and Prometheus included: `docker compose -f docker-compose.yml -f loadtest/compose.load.yml --profile load down -v`.

## Running the generator outside the container

```sh
go run ./cmd/loadgen seed -scenario smoke -out loadtest/results/manual
go run ./cmd/loadgen run -scenario smoke -out loadtest/results/manual
go run ./cmd/loadgen verify -out loadtest/results/manual
go run ./cmd/loadgen report -out loadtest/results/manual
go run ./cmd/loadgen baseline -out loadtest/results/manual-baseline   # MiniStack ceiling
```

The defaults point to the ports published on the host (`localhost:8091..8093`, Keycloak on `8081`, MiniStack on `4566`) and the database on `localhost:${POSTGRES_HOST_PORT:-55432}`; everything can be changed by flag or variable (`LOADGEN_TARGETS`, `LOADGEN_TOKEN_URL`, `LOADGEN_DATABASE_URL`, `AWS_ENDPOINT_URL`).
SQS needs `AWS_ACCESS_KEY_ID=000000000000` and `AWS_SECRET_ACCESS_KEY=test` in the environment; `-sqs=false` turns the channel off.
On Docker Desktop, host ports go through a proxy that adds latency; the reference numbers are the ones from the container.

Generator tests (histogram, scheduler, tokens, oracle and a full run against an in-memory fake service):

```sh
go test ./cmd/loadgen/...
```

## Known limits

- **MiniStack is an emulator, and it is the bottleneck.** It runs on a single thread and the cost of each operation grows with the queue depth (section [Emulator ceiling](#emulator-ceiling)); on this machine, the full outbox path (SNS publish plus fan-out plus consumption) saturates around 110 to 130 events/s, about 60 to 70 op/s of the default mix.
  Above that, outbox lag and SQS end-to-end latency grow while the instances stay below 7% CPU: what is measured is the emulator, not the service, and the SQS and SNS numbers say nothing about AWS.
  To move the bottleneck away from the emulator and measure the service itself, use the [limited resources](#limited-resources).
- **A single host.** Generator, three instances, Postgres, Keycloak and MiniStack share the same machine (and, on Docker Desktop, the same VM); the generator competes for CPU with what it measures.
  The generator CPU cap and the saturation alarm reduce the effect, but the numbers are for comparing versions on the same machine, not production capacity.
- **FIFO serializes per wallet.** With `MessageGroupId = walletId`, a hot wallet over SQS moves one message at a time; that is why `hot-wallet` uses HTTP only.
- **Connection pool.** With `DB_MAX_CONNS=10` per instance, the pool saturates before the database; the report records the value used.
- **Orphans only with a short TTL.** Reversals without a reference take the service TTL (10 min by default) to finish; `-orphan-ratio` only makes sense with a reduced TTL on the service.
- **Database samples every 2 s.** Short lock waits may not show up in the count of ungranted locks; the dashboard and `pg_stat_statements` fill the gap.

## Emulator ceiling

`loadtest/run.sh baseline` measures MiniStack alone, without the service, with 8 callers and 2,000 `MessageGroupId`s (like `walletId`).
Result on the reference machine (20 s phases; first and second-to-last second of each phase):

| Phase | Mean (msg/s) | At start (msg/s) | At end (msg/s) | MiniStack CPU |
| --- | ---: | ---: | ---: | ---: |
| SQS `SendMessageBatch`, queue growing from 0 to 18 thousand | 914 | 3,710 | 460 | ~100% |
| SQS `ReceiveMessage` + `DeleteMessageBatch`, queue emptying | 862 | 450 | 2,090 | ~100% |
| SNS `Publish`, with fan-out to the audit queue | 283 | 249 | 312 | ~100% |
| SNS `PublishBatch` (10 per call), with fan-out | 397 | 410 | 400 | ~100% |

Two conclusions.
MiniStack takes a full core (102% mean, 109% max) while the wallet instances stay below 1%: it is single-threaded, so the ceiling does not rise with more CPU on the machine.
And the cost per operation grows with queue depth: sending drops from 3,710 to 460 msg/s as the queue fills, and receiving speeds up from 450 to 2,090 msg/s as it empties.
That is why a queue nobody consumes (like the audit queue, which piled up tens of thousands of events between runs) makes all messaging slower, and the generator now empties and consumes it on every run.

## Results

Reference runs from 2026-10-21, each with `loadtest/run.sh <scenario>` against the running stack, in sequence, with the defaults.
Environment: Intel Core i7-13650HX (20 threads), 31 GiB, Linux 6.17, Docker Desktop 28.5.1 with an 8-CPU, 11.7 GiB VM, Compose 2.40, Postgres 17.11 with `fsync=on` and `synchronous_commit=on`, `DB_MAX_CONNS=10` per instance, generator capped at 2 CPUs, commit `52c451c`.
Every run ended in `PASS`: the 15 SQL invariants with zero violations, zero violations in the cross-check with the journal, consistent reconciliation on every sampled wallet, outbox drained and no DLQ growth.
The full `steady` report is in [`results/samples/steady/report.md`](results/samples/steady/report.md).

| Scenario | Throughput (op/s) | HTTP p50 / p95 / p99 (ms) | SQS send p50 / p95 / p99 (ms) | SQS end to end p50 / p95 / p99 (ms) | Final errors | Conflicts | Outbox lag p50 / p95 / p99 (s) | MiniStack CPU mean / max | CPU per instance |
| --- | ---: | --- | --- | --- | ---: | --- | --- | --- | ---: |
| `smoke` | 20.5 | 3.7 / 16.9 / 24.3 | 9.3 / 15.9 / 24.8 | 26 / 80 / 117 | 0 | 22 × 409 (probes), 56 replays | 0.10 / 0.21 / 0.22 | 16% / 21% | 1% |
| `steady` | 30.0 | 5.3 / 11.5 / 35.8 | 9.7 / 24.3 / 37.4 | 43 / 105 / 129 | 0 | none | 0.11 / 0.22 / 0.24 | 24% / 34% | 3% |
| `hot-wallet` | 60.0 | 5.1 / 10.4 / 26.9 | — | — | 0 | 218 × 422 `INSUFFICIENT_FUNDS` | 0.14 / 0.29 / 0.46 | 39% / 57% | 4–5% |
| `duplicates` | 60.2 | 2.3 / 8.2 / 13.2 | 10.6 / 16.4 / 28.4 | 24 / 73 / 96 | 0 | 550 × 409 (probes, all with the expected code), 1,429 replays | 0.11 / 0.21 / 0.21 | 22% / 38% | 3% |
| `reversal-first` | 20.0 | 5.9 / 10.0 / 13.4 | — | — | 0 | none; reversal resolution p50 406 ms, p99 614 ms | 0.12 / 0.24 / 0.27 | 25% / 34% | 4% |
| `ramp` | 80.0 (mean from 20 to 140) | 4.0 / 10.1 / 50.7 | 401 / 1,294 / 2,884 | 1,884 / 44,564 / 46,662 | 0 | none | 5.35 / 29.29 / 32.33 | 64% / 118% | 5% |

In `hot-wallet`, the three wallets received 20 op/s each, from the three instances at the same time; none went negative, and the balance and version of each matched the values recomputed from the responses, meaning no lost update.
In `duplicates`, each operation had exactly one effect in the database, despite 10 sends over HTTP across the three instances and over SQS; every HTTP copy returned the same `transactionId` and the same balance.

`ramp` steps (knee: last step with client p99 below 500 ms and errors below 1%):

| Target (op/s) | p50 (ms) | p99 (ms) | Largest outbox backlog | MiniStack CPU | Within |
| ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 5.6 | 145 | 21 | 41% | yes |
| 40 | 6.3 | 455 | 80 | 85% | yes |
| 60 | 4.7 | 381 | 86 | 80% | yes |
| 80 | 4.3 | 1,589 | 349 | 102% | no |
| 100 | 4.5 | 1,442 | 955 | 104% | no |
| 120 | 4.7 | 1,802 | 2,216 | 104% | no |
| 140 | 4.0 | 991 | 4,600 | 105% | no |

Knee: **60 op/s**.

`steady` calibration runs above the ceiling, which led to the 30 op/s default:

| Rate | Verdict | Events generated / published per s during load | Largest backlog | Outbox lag p99 | MiniStack CPU | CPU per instance / Postgres |
| ---: | --- | --- | ---: | ---: | ---: | --- |
| 30 op/s | `PASS` | 53 / 53 | 37 | 0.46 s | 56% | 2% / 13% |
| 60 op/s | `PASS` | 105 / 102 | 1,151 | 11 s | 105% | 5% / 31% |
| 100 op/s (1 min) | `PASS` (only because it drained in 34 s) | 164 / 112 | 3,093 | 25 s | 103% | 4% / 30% |
| 200 op/s | `FAIL`: did not drain in 2 min | 314 / 136 | 21,204 | — | 104% | 6% / 38% |

### Interpretation

The bottleneck is MiniStack.
On every step and at every rate above ~60 op/s, it sits at 100% to 105% CPU, that is, a full core of a single-threaded process, while each wallet instance uses 2% to 7% and Postgres 13% to 38%, with plenty of headroom on the 8-CPU VM.
The symptom shows up in messaging, not in the API: HTTP p50 latency stays between 4 and 6 ms on every `ramp` step, but the SQS send goes from 10 ms to more than 1 s, the outbox starts publishing fewer events than it generates (112 against 164 per second at 100 op/s), and the backlog grows step by step.
The `baseline` confirms the ceiling belongs to the emulator: with none of the service's logic, it already takes a full core and gets slower as the queues grow.
The capacity of the wallet itself (HTTP, locks, database) is above what this stack can show, and was not measured: every operation generates events that go through SNS, so even an HTTP-only load runs into MiniStack; measuring it would require a real broker (AWS) or a multi-threaded emulator.
Below the ceiling, correctness does not depend on the rate: every run, including the 100 op/s one with 25 s of outbox lag, ended with zero violations.
