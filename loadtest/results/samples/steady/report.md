# Load report: steady

Result: **PASS**.
Run `lg-steady-20261021T022849`, from 2026-10-21 02:28:55 to 02:30:55 (UTC).
All invariants and the cross-check against the journal passed, and the generator kept its schedule.

## How to reproduce

From the repository root:

```sh
loadtest/run.sh steady
```

Generator command for this step:

```sh
loadgen run -scenario steady -out /results/20261021T022840Z-steady
```

## Environment

```text
date_utc: 2026-10-21T02:28:44Z
git_commit: 52c451c
host: Linux 6.17.0-19-generic x86_64
cpu: 13th Gen Intel(R) Core(TM) i7-13650HX x 20
ram_host: 31.0 GiB
docker: 28.5.1 (Docker Desktop, 8 CPUs, 12540080128 bytes for containers)
compose: 2.40.0-desktop.1
generator_cpus: 2
db_max_conns_per_instance: 10 (default)
postgres: 17.11
postgres_settings: fsync=on full_page_writes=on max_connections=100 shared_buffers=16384 synchronous_commit=on wal_level=replica
images:
  CONTAINER                                 REPOSITORY                                      TAG                 PLATFORM            IMAGE ID            SIZE                CREATED
  wallet-service-grafana-1                  grafana/grafana                                 12.1.1              linux/amd64         a1701c218024        202MB               45 minutes ago
  wallet-service-keycloak-1                 quay.io/keycloak/keycloak                       26.8.1              linux/amd64         79c623a22aba        266MB               13 hours ago
  wallet-service-migrate-1                  wallet-service                                  local               linux/amd64         37d5a4b7e1fa        15MB                About a minute ago
  wallet-service-ministack-1                ministackorg/ministack                          1.5.30              linux/amd64         9019a8ab2531        69.4MB              13 hours ago
  wallet-service-postgres-1                 postgres                                        17.11-alpine        linux/amd64         b0f9560a2de0        117MB               13 hours ago
  wallet-service-postgres-exporter-1        quay.io/prometheuscommunity/postgres-exporter   v0.17.1             linux/amd64         38606faa38c5        11.2MB              45 minutes ago
  wallet-service-prometheus-1               prom/prometheus                                 v3.5.0              linux/amd64         63805ebb8d2b        124MB               45 minutes ago
  wallet-service-provision-1                wallet-service                                  local               linux/amd64         37d5a4b7e1fa        15MB                About a minute ago
  wallet-service-wallet-1-1                 wallet-service                                  local               linux/amd64         37d5a4b7e1fa        15MB                About a minute ago
  wallet-service-wallet-2-1                 wallet-service                                  local               linux/amd64         37d5a4b7e1fa        15MB                About a minute ago
  wallet-service-wallet-3-1                 wallet-service                                  local               linux/amd64         37d5a4b7e1fa        15MB                About a minute ago
```

Scenario parameters:

| Parameter | Value |
| --- | --- |
| drain_audit_queue | true |
| duration | 2m0s |
| hot_wallets | 0 |
| http_attempt_timeout | 5s |
| http_max_attempts | 6 |
| max_in_flight | 4000 |
| mix | map[round-http:80 round-sqs:20] |
| rate_ops_per_sec | 30 |
| scenario | steady |
| sqs | true |
| wallets | 1000 |
| warmup | 20s |
| instances | http://wallet-1:8080, http://wallet-2:8080, http://wallet-3:8080 |

## Methodology

Open model: arrivals follow a constant rate, independent of the responses, and latency is measured from the scheduled instant (no coordinated omission).
Client latency includes retries: `503` responses and timeouts are retried with the same `Idempotency-Key`, with exponential backoff and jitter.
Operations scheduled during the warm-up are left out of the statistics, but they are journaled and verified.
Within a round, the first operation is measured from its scheduled instant and the next one from the moment the provider would send it (after the bet response).
On SQS, client latency is only the send to the broker; the end-to-end latency (scheduled until the row is written) comes from the database.
Postgres runs with `fsync` and `synchronous_commit` on.
Generator, service and dependencies share the same host; the numbers are for comparing versions on the same machine, not a production capacity figure.

## Throughput

| Metric | Value |
| --- | ---: |
| measured operations | 3000 |
| measured window (s) | 100.0 |
| throughput (op/s) | 30.0 |
| throughput http (op/s) | 24.2 |
| throughput sqs (op/s) | 5.8 |
| wallets opened (POST /wallets) | 1000 in 0.9s (1130.6/s) |

## Client latency

By channel:

| Channel | Kind | N | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| http | * | 2416 | 5.3 | 11.5 | 35.8 | 95.2 | 116.1 |
| sqs | * | 584 | 9.7 | 24.3 | 37.4 | 99.3 | 99.3 |

By channel and kind:

| Channel | Kind | N | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| http | BET | 1208 | 7.0 | 12.5 | 38.4 | 111.6 | 116.1 |
| http | LOSS | 550 | 3.6 | 6.5 | 20.2 | 45.1 | 45.1 |
| http | REFUND | 62 | 5.2 | 9.5 | 35.3 | 35.3 | 35.3 |
| http | ROLLBACK | 66 | 4.5 | 8.8 | 9.2 | 9.2 | 9.2 |
| http | WIN | 530 | 4.7 | 9.1 | 32.0 | 40.6 | 40.6 |
| sqs | BET | 292 | 11.0 | 30.2 | 48.6 | 99.3 | 99.3 |
| sqs | LOSS | 134 | 9.1 | 21.8 | 27.1 | 29.5 | 29.5 |
| sqs | REFUND | 17 | 8.8 | 34.0 | 34.0 | 34.0 | 34.0 |
| sqs | ROLLBACK | 15 | 9.2 | 11.2 | 11.2 | 11.2 | 11.2 |
| sqs | WIN | 126 | 8.6 | 10.6 | 26.1 | 32.9 | 32.9 |

By channel, kind and outcome:

| Channel | Kind | Outcome | N | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| http | BET | PROCESSED | 1208 | 7.0 | 12.5 | 38.4 | 111.6 | 116.1 |
| http | LOSS | PROCESSED | 550 | 3.6 | 6.5 | 20.2 | 45.1 | 45.1 |
| http | REFUND | PROCESSED | 62 | 5.2 | 9.5 | 35.3 | 35.3 | 35.3 |
| http | ROLLBACK | PROCESSED | 66 | 4.5 | 8.8 | 9.2 | 9.2 | 9.2 |
| http | WIN | PROCESSED | 530 | 4.7 | 9.1 | 32.0 | 40.6 | 40.6 |
| sqs | BET | SENT | 292 | 11.0 | 30.2 | 48.6 | 99.3 | 99.3 |
| sqs | LOSS | SENT | 134 | 9.1 | 21.8 | 27.1 | 29.5 | 29.5 |
| sqs | REFUND | SENT | 17 | 8.8 | 34.0 | 34.0 | 34.0 | 34.0 |
| sqs | ROLLBACK | SENT | 15 | 9.2 | 11.2 | 11.2 | 11.2 | 11.2 |
| sqs | WIN | SENT | 126 | 8.6 | 10.6 | 26.1 | 32.9 | 32.9 |

Other latencies:

| Measure | N | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| SQS end to end (scheduled until the transaction is written) | 584 | 43.0 ms | 105.5 ms | 129.0 ms | 164.6 ms |
| event delivered to the consumer (occurredAt until received on the audit queue) | 6379 | 120.8 ms | 227.3 ms | 270.3 ms | 346.0 ms |
| wallet opening (POST /wallets) | 1000 | 20.7 ms | 77.8 ms | 99.3 ms | 210.1 ms |

## Errors and conflicts

| Measure | Value |
| --- | ---: |
| final errors (no response, final 5xx or unexpected 4xx) | 0 (0.00%) |
| HTTP retries (extra attempts) | 0 |
| retried 503 responses | 0 |
| SQS batches sent / batch retries | 712 / 0 |
| HTTP idempotent replays (`idempotentReplay: true`) | 0 |
| 409 responses | 0 |
| max sessions waiting on a lock (sampled every 2 s) | 0 |
| logical operations / copies / 409 probes | 3602 / 0 / 0 |
| outcome unknown to the client, applied / not applied | 0 / 0 |
| final status in the database `PROCESSED` | 3602 |

## Outbox

Publication delay (`published_at − occurred_at`), computed in SQL over every event in the run window.

| Measure | Value |
| --- | ---: |
| events | 6383 |
| unpublished | 0 |
| p50 (s) | 0.11 |
| p95 (s) | 0.22 |
| p99 (s) | 0.24 |
| max (s) | 0.30 |
| events produced per second during the load | 53.2 |
| events published per second during the load | 53.0 |
| largest backlog during the load | 14 |
| outbox / input queue backlog at the end of the load | 0 / 0 |
| events consumed from the audit queue | 8379 |
| drain after the end of the load (s) | 3.8 |

## Verification

SQL invariants over the whole database (`cmd/loadgen/internal/verify/invariants.sql`), in one `REPEATABLE READ READ ONLY` transaction:

| Invariant | Description | Violations |
| --- | --- | ---: |
| `balance_equals_ledger` | stored balance = sum of credits − sum of debits in the ledger | 0 |
| `version_equals_last_entry` | wallet version = highest wallet_version in the ledger (1 when there is no entry) | 0 |
| `ledger_chain_continuity` | ledger chain without gaps: version n = (n−1) + 1 and balance_before(n) = balance_after(n−1) | 0 |
| `no_negative_balance` | no negative balance, in the wallet or at any point of the ledger | 0 |
| `no_duplicate_operation` | a single transaction per (provider, externalTransactionId) and per (provider, Idempotency-Key) | 0 |
| `processed_moving_has_one_entry` | every PROCESSED transaction that moves the balance has exactly one entry, with the same amount and resulting balance | 0 |
| `non_moving_has_no_entry` | LOSS, REJECTED, FAILED and pending transactions have no entry | 0 |
| `entry_direction_matches_kind` | entry direction matches the kind (ROLLBACK: opposite to the reference) | 0 |
| `one_successful_reversal_per_reference` | at most one PROCESSED reversal (REFUND or ROLLBACK) per reference | 0 |
| `reversal_matches_reference` | a processed reversal has the same wallet and amount as its reference | 0 |
| `no_failed_transactions` | no FAILED transaction (e.g. INVARIANT_VIOLATION) and no persisted PENDING | 0 |
| `no_stuck_pending_reference` | no PENDING_REFERENCE expired for more than 30 s (the worker should have rejected it) | 0 |
| `outbox_fully_published` | outbox drained: no event without published_at | 0 |
| `outbox_events_match_state` | one WagerTransactionProcessed per PROCESSED, one WalletBalanceChanged per entry, one WagerTransactionRejected per REJECTED | 0 |
| `inbox_all_completed` | every received SQS message was completed | 0 |

Cross-check against the journal (independent oracle): 1000 wallets checked, 0 violations.

Reconciliation through the API: 50 wallets, 0 inconsistent or failed.
Lookup by `(providerId, externalTransactionId)`: 50 transactions, 0 differing from the database.
DLQ: 0 messages after the run (change 0).

## CPU and memory per service

`docker stats` samples every ~2 s; 100% is one full core.
A single-threaded service near 100% (the MiniStack) is a bottleneck even with the machine idle; with `LIMITS=1`, the ceiling of each capped service is its CPU limit (0.25 core = 25%), recorded in `env.txt`.
Memory: peak during the run against the container limit (without a limit, Docker shows the VM memory).

| Service | Load: mean | Load: max | Drain and verification: mean | Drain and verification: max | Max memory / limit (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| ministack | 23.9% | 34.5% | 0.0% | 0.0% | — |
| postgres | 11.9% | 22.0% | 0.0% | 0.0% | — |
| wallet-1 | 2.8% | 4.4% | 0.0% | 0.0% | — |
| wallet-3 | 2.7% | 4.7% | 0.0% | 0.0% | — |
| loadgen | 2.7% | 3.9% | 0.0% | 0.0% | — |
| wallet-2 | 2.6% | 4.5% | 0.0% | 0.0% | — |
| postgres-exporter | 0.6% | 2.0% | 0.0% | 0.0% | — |
| keycloak | 0.5% | 2.4% | 0.0% | 0.0% | — |
| grafana | 0.4% | 2.5% | 0.0% | 0.0% | — |
| prometheus | 0.3% | 0.5% | 0.0% | 0.0% | — |

## Generator

| Measure | Value |
| --- | ---: |
| scheduled tasks | 1801 |
| dropped tasks (generator saturated) | 0 |
| tasks abandoned at the end | 0 |
| dispatch lag p99 / max (ms) | 2.1 / 24.8 |
| journal lines | 3602 |
| tokens requested from Keycloak by `provider-a` | 1 |
| tokens requested from Keycloak by `provider-b` | 1 |

## Most expensive queries (pg_stat_statements)

```text
2258.4 | 4602 | 0.491 | INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id
2218.5 | 3781 | 0.587 | INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before, balance_after, wallet_v
570.5 | 8383 | 0.068 | INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload, occurred_at, next_attempt_at) VALUES ($1,$2
554.9 | 1821 | 0.305 | UPDATE outbox_events SET locked_by = $1, locked_until = now() + $2::interval, attempts = attempts + 1 WHERE id IN ( SELECT id FROM outbox_ev
284.4 | 7204 | 0.039 | SELECT id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payloa
282.4 | 938 | 0.301 | UPDATE outbox_events SET published_at = now(), locked_by = $3, locked_until = $4, last_error = $5 WHERE id = ANY($1) AND locked_by = $2 AND 
191.8 | 7451 | 0.026 | SELECT id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payloa
166.9 | 3602 | 0.046 | SELECT id, player_id, currency, balance_minor, version, created_at, updated_at FROM wallets WHERE id = $1 FOR UPDATE
166.2 | 66 | 2.519 | SELECT current_database() datname, schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_
162.5 | 2781 | 0.058 | UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5
```
