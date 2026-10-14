#!/usr/bin/env bash
# Creates labels, milestones and issues for go-resilient-workers.
# Usage: scripts/create-issues.sh [owner/repo]   (requires: gh auth login)
set -euo pipefail
REPO="${1:-gapfware/go-resilient-workers}"

echo "Labels..."
gh label create "core" --repo "$REPO" --color "1d76db" --description "worker, retry, idempotency, offsets" --force >/dev/null
gh label create "outbox" --repo "$REPO" --color "5319e7" --description "transactional outbox" --force >/dev/null
gh label create "kafka" --repo "$REPO" --color "0e8a16" --description "Kafka adapter module" --force >/dev/null
gh label create "postgres" --repo "$REPO" --color "006b75" --description "Postgres stores" --force >/dev/null
gh label create "observability" --repo "$REPO" --color "fbca04" --description "logging, metrics, tracing" --force >/dev/null
gh label create "example" --repo "$REPO" --color "c5def5" --description "runnable examples" --force >/dev/null
gh label create "ci" --repo "$REPO" --color "bfd4f2" --description "CI / tooling" --force >/dev/null
gh label create "docs" --repo "$REPO" --color "0075ca" --description "documentation" --force >/dev/null
gh label create "starter-code" --repo "$REPO" --color "d4c5f9" --description "already implemented in the starter code: review and close" --force >/dev/null
gh label create "good first issue" --repo "$REPO" --color "7057ff" --description "good for newcomers" --force >/dev/null

echo "Milestones..."
gh api "repos/$REPO/milestones" -f title="v0.1.0" -f description="First public release: core worker, outbox with Postgres, Kafka adapter, examples, CI, docs" >/dev/null 2>&1 || echo "  milestone v0.1.0 already exists"
gh api "repos/$REPO/milestones" -f title="v0.2.0" -f description="Ecosystem: observability adapters, more stores and brokers, benchmarks" >/dev/null 2>&1 || echo "  milestone v0.2.0 already exists"

echo "Issues..."
gh issue create --repo "$REPO" --title "core: retry policies" --label "core" --label "starter-code" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_0'
## Context
Workers and the outbox relay need a shared way to decide whether to retry and how long to wait.

## Scope
- `retry.Policy` interface: `Backoff(attempt int) (time.Duration, bool)`; `attempt` = failures so far.
- `Exponential{Initial, Max, Multiplier, MaxAttempts, Jitter}` with sane defaults, overflow-safe cap, injectable `Rand` for tests.
- `Constant(delay, maxAttempts)`, `Never()`, `Default()` (5 attempts, 200ms→10s, 20% jitter).

## Acceptance criteria
- [ ] Growth, cap, max-attempts, jitter bounds and huge-attempt overflow covered by tests.
- [ ] Doc comments explain why jitter matters (thundering herd).
ISSUE_BODY_0
gh issue create --repo "$REPO" --title "core: idempotency store with leases" --label "core" --label "starter-code" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_1'
## Context
At-least-once delivery means duplicates. Handlers must be protected without the caller writing dedup logic.

## Scope
- `idempotency.Store`: `Acquire(key, lease)`, `Complete(key, ttl)`, `Release(key)`; errors `ErrAlreadyProcessed`, `ErrInProgress`.
- `MemoryStore` (mutex + map, injectable clock, `Sweep()`).

## Acceptance criteria
- [ ] Lifecycle, release-after-failure, expired-lease takeover, TTL expiry tested.
- [ ] 64 concurrent `Acquire` on the same key → exactly one winner (`-race`).
- [ ] `Release` never forgets a completed key.
ISSUE_BODY_1
gh issue create --repo "$REPO" --title "core: worker runtime" --label "core" --label "starter-code" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_2'
## Context
The heart of the library. See the delivery contract in CLAUDE.md.

## Scope
- `worker.Source` (`Fetch`, `Commit`), `DeadLetter`, `Handler`/`HandlerFunc`, `Middleware`, `Message` (with `Attempt`, `Raw`).
- Errors: `Permanent`, `IsPermanent`, `RetryAfter`, `PanicError`, `ErrRetriesExhausted`, `ErrShutdownTimeout`, `ErrAlreadyRunning`, `ErrSourceClosed`.
- Options: concurrency, key ordering (lanes by FNV hash of key), retry policy, DLQ + DLQ retry policy (default: retry until shutdown), idempotency (`KeyFromHeader`, `KeyFromMessageKey`), handler timeout, shutdown timeout, middleware, observers (additive), fetch error backoff.
- Graceful shutdown: stop fetching on cancel; in-flight handlers keep `context.WithoutCancel`; messages in backoff are abandoned uncommitted; timeout cancels handlers.

## Acceptance criteria
- [ ] Tests: success, transient retry, permanent → DLQ without retry, exhausted → DLQ wrapping `ErrRetriesExhausted`, no DLQ → dropped, duplicates skipped, key released after failure, panic recovered, `RetryAfter` overrides delay, handler timeout retryable, DLQ outage retried, shutdown waits for in-flight, shutdown abandons backoff, shutdown timeout, key ordering under concurrency 8, middleware order, double `Run`.
- [ ] All green with `-race -count=10`.
ISSUE_BODY_2
gh issue create --repo "$REPO" --title "core: offset tracker for contiguous commits" --label "core" --label "kafka" --label "starter-code" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_3'
## Context
With concurrency > 1, offsets finish out of order. Committing the highest finished offset can lose messages on crash.

## Scope
- `offsets.Tracker`: `Track(partition, offset)`, `Done(partition, offset) (commit int64, ok bool)`, `Pending()`, `Forget(partition)`.
- Rewind (offset <= last tracked) resets the partition.

## Acceptance criteria
- [ ] Out-of-order completion, independent partitions, rewind, unknown offsets tested.
- [ ] 2000 offsets completed concurrently in random order → final commit is the last offset, nothing pending.
ISSUE_BODY_3
gh issue create --repo "$REPO" --title "core: tests for the in-memory transport" --label "core" --label "good first issue" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_4'
## Context
`memory.Queue` and `memory.DeadLetterQueue` are implemented but have no direct tests (they are only exercised through worker tests).

## Acceptance criteria
- [ ] `Publish` fills `ID`/`Topic` when empty and copies the message.
- [ ] `Fetch` returns `worker.ErrSourceClosed` after `Close` once the buffer is drained; respects ctx cancellation.
- [ ] `Commit` is idempotent; `Committed()` / `Uncommitted()` are accurate.
- [ ] `Publish` after `Close` returns `ErrQueueClosed`.
- [ ] `DeadLetterQueue.Entries()` returns copies.
ISSUE_BODY_4
gh issue create --repo "$REPO" --title "outbox: tests for Relay and MemoryStore" --label "outbox" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_5'
## Context
`outbox.Relay` and `outbox.MemoryStore` are written in the starter code but untested.

## Acceptance criteria
- [ ] Happy path: enqueue N records, `Flush` publishes all in insertion order and marks them published.
- [ ] Per-key ordering: record 1 of key K fails → records 2..n of K in the same batch are released (attempts unchanged), other keys still publish; next `Flush` before the retry delay does not return any K record; after the delay K publishes in order.
- [ ] Records without key are never blocked by failures.
- [ ] `MarkFailed` increments `Attempts`; error handler receives `*PublishError` with `RetryIn` from the policy.
- [ ] Context cancelled mid-batch: already published records are still marked published, the rest released.
- [ ] `Run` drains a backlog larger than the batch size without waiting for the poll interval, and returns nil on cancel.
- [ ] Inject a clock into `MemoryStore` for deterministic lease/retry tests.
ISSUE_BODY_5
gh issue create --repo "$REPO" --title "outbox: Postgres store" --label "outbox" --label "postgres" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_6'
## Context
The outbox is only useful with a real database: the record must be inserted in the same transaction as the business change.

## Scope (package `outbox/postgres`, `database/sql`, no driver dependency in non-test code)
- `Schema` (or `Migrate(ctx, db)`): table `outbox(id BIGSERIAL PK, topic TEXT, key BYTEA, payload BYTEA, headers JSONB, created_at, published_at, locked_until, attempts INT, last_error TEXT)` + partial indexes on pending `(id)` and `(key, id)`. Table name configurable and validated as an identifier.
- `Enqueue(ctx, q Querier, rec) (id string, err error)` where `Querier` is satisfied by both `*sql.DB` and `*sql.Tx`.
- `Claim`: in one transaction, `pg_advisory_xact_lock(<table hash>)`, then select pending, unlocked records whose key has no older pending record that is locked/backing off, `ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, set `locked_until`; return sorted by id.
- `MarkPublished`, `Release` using `id = ANY($1::bigint[])` with a text array literal (driver agnostic).
- `MarkFailed`: `attempts+1`, `last_error`, `locked_until = now() + retryIn`.
- `Prune(ctx, olderThan)` deletes published records.

## Acceptance criteria
- [ ] Integration tests (`-tags integration`, `DATABASE_URL`, `lib/pq` test-only) cover: enqueue inside a rolled-back tx leaves nothing; ordering per key; two relays running concurrently over 1000 records across 20 keys publish every record exactly once per successful attempt and in per-key order; lease expiry makes records claimable again; prune.
- [ ] Same behavioural test suite runs against `MemoryStore` and Postgres (shared conformance tests in `outbox/outboxtest`).
ISSUE_BODY_6
gh issue create --repo "$REPO" --title "kafka: adapter module (Source, Publisher, DeadLetter)" --label "kafka" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_7'
## Context
First broker adapter. Lives in its own module so the core stays dependency-free.

## Scope (`kafka/go.mod`, module `github.com/gapfware/go-resilient-workers/kafka`, `github.com/segmentio/kafka-go`)
- `Source` wrapping a `*kafka.Reader` with `GroupID`: `Fetch` uses `FetchMessage`, tracks offsets in `offsets.Tracker`, maps headers, sets `ID = topic/partition/offset`, `Raw = kafka.Message`. `Commit` marks done and calls `CommitMessages` with the new contiguous watermark; commits are serialised with a mutex so they never regress.
- `Publisher` (implements `outbox.Publisher`) over `*kafka.Writer`.
- `DeadLetter` (implements `worker.DeadLetter`): writes to a DLQ topic preserving key/value and adding headers `x-dlq-reason`, `x-dlq-attempts`, `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-failed-at`.
- Document the rebalance limitation of kafka-go (no revoke callbacks) and how the tracker's rewind detection mitigates it.

## Acceptance criteria
- [ ] Unit tests with a fake reader/writer interface.
- [ ] Integration test (`-tags integration`, `KAFKA_BROKERS`): produce 500 messages, process with concurrency 8 where some fail and some go to the DLQ, restart the consumer mid-way, assert every message is either handled or in the DLQ and committed offsets never skip an unfinished message.
- [ ] `replace` directive to `../` for local development; documented how to drop it for releases.
ISSUE_BODY_7
gh issue create --repo "$REPO" --title "example: provisioning workflow without infrastructure" --label "example" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_8'
## Context
`go run ./examples/provisioning` should show every feature in 30 seconds without Docker.

## Scope
- Fictional domain: account activation requests for an ISP (`activate`, `suspend`, `cancel`).
- A fake external provider that fails randomly (retryable), rate-limits (`RetryAfter`), and rejects unknown plans (`Permanent`).
- Duplicate requests with the same `Idempotency-Key` header.
- Concurrency 4 with key ordering by account id, `SlogObserver` with text handler, `Stats` printed at the end, Ctrl+C triggers graceful shutdown.

## Acceptance criteria
- [ ] Deterministic with `-seed` flag; prints a final summary table (succeeded / retried / duplicates / dead-lettered).
- [ ] Referenced from the README with sample output.
ISSUE_BODY_8
gh issue create --repo "$REPO" --title "example: Kafka + Postgres end-to-end with docker compose" --label "example" --label "kafka" --label "postgres" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_9'
## Scope
- `docker-compose.yml` at repo root: Kafka (apache/kafka, KRaft single node) + Postgres 16, healthchecks, topics created on start (`provisioning.requests`, `provisioning.requests.dlq`, `provisioning.events`).
- `kafka/examples/end-to-end`: an HTTP endpoint inserts a provisioning request + outbox record in one transaction; the relay publishes to Kafka; a worker consumes, calls the fake provider, emits a result event via the outbox.
- `make up`, `make demo`, `make down`.

## Acceptance criteria
- [ ] Killing the worker mid-run and restarting it loses nothing and double-applies nothing (shown in README).
ISSUE_BODY_9
gh issue create --repo "$REPO" --title "ci: GitHub Actions" --label "ci" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_10'
## Scope
- Workflow on push/PR: matrix Go 1.22 and stable; `gofmt` check, `go vet`, `staticcheck`, `go test -race -count=3 ./...` for root and `kafka/` modules; coverage uploaded as artifact.
- Integration job with service containers (Postgres 16, Kafka KRaft) running `-tags integration`.
- `govulncheck` job.
- Dependabot for Go modules and Actions.

## Acceptance criteria
- [ ] Badges in README (CI, Go Reference, Go Report Card, license).
- [ ] Branch protection recommendation documented in CONTRIBUTING.
ISSUE_BODY_10
gh issue create --repo "$REPO" --title "docs: README and design notes" --label "docs" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_11'
## Scope
- README (English): one-paragraph pitch, feature list, install, 20-line quick start, delivery contract, options table, how retries/DLQ/idempotency/ordering/shutdown interact, examples, roadmap, license.
- `docs/design.md`: message lifecycle state diagram (Mermaid), why contiguous offset commits, why the outbox relay releases instead of failing, trade-offs (head-of-line blocking with key ordering, at-least-once vs exactly-once).
- Runnable `Example*` functions in `worker` and `outbox` packages so pkg.go.dev shows them.

## Acceptance criteria
- [ ] A reader can understand the guarantees without reading code.
- [ ] Every snippet in the README compiles (covered by example tests).
ISSUE_BODY_11
gh issue create --repo "$REPO" --title "chore: repository hygiene" --label "docs" --label "good first issue" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_12'
## Scope
- `LICENSE` (MIT), `CONTRIBUTING.md`, `CHANGELOG.md` (Keep a Changelog), issue and PR templates, `.gitignore`, `Makefile` (`test`, `test-integration`, `lint`, `up`, `down`, `demo`), `.editorconfig`.
- GitHub repo settings: description, topics (`go`, `golang`, `kafka`, `outbox-pattern`, `event-driven`, `retry`, `dead-letter-queue`, `idempotency`), website = pkg.go.dev link.
ISSUE_BODY_12
gh issue create --repo "$REPO" --title "release: v0.1.0" --label "docs" --milestone "v0.1.0" --body-file - <<'ISSUE_BODY_13'
## Scope
- Tag `v0.1.0` (root) and `kafka/v0.1.0`; remove the `replace` directive in `kafka/go.mod` before tagging.
- Verify both modules on pkg.go.dev; GitHub release notes from CHANGELOG.
- Pin the repo on the GitHub profile; short LinkedIn post linking to it with the design notes.
ISSUE_BODY_13
gh issue create --repo "$REPO" --title "observability: OpenTelemetry observer and middleware" --label "observability" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_14'
## Scope
- Separate module `otel/`: span per message (extracting trace context from headers via a propagator), span events per retry, metrics: `messages_total{outcome}`, `attempts`, `processing_duration`, `in_flight`.
- Outbox relay metrics: pending, publish failures, lag (now - oldest pending `created_at`).
ISSUE_BODY_14
gh issue create --repo "$REPO" --title "observability: Prometheus observer" --label "observability" --label "good first issue" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_15'
Separate module `prometheus/` exposing the same metrics as the OTel observer through `prometheus/client_golang`. Include a Grafana dashboard JSON in `docs/`.
ISSUE_BODY_15
gh issue create --repo "$REPO" --title "idempotency: Redis store" --label "core" --label "enhancement" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_16'
Separate module `redisstore/` (`go-redis/v9`). `Acquire` with `SET key leased NX PX`, `Complete` with `SET key done PX ttl`, `Release` with a Lua compare-and-delete so a worker never releases a lease that expired and was taken over. Conformance test suite shared with `MemoryStore` (`idempotency/idempotencytest`).
ISSUE_BODY_16
gh issue create --repo "$REPO" --title "idempotency: Postgres store" --label "core" --label "postgres" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_17'
`idempotency/postgres` using `INSERT ... ON CONFLICT` with lease expiry, so the idempotency key can be completed in the same transaction as the handler's business write (true effectively-once for DB side effects). Document this pattern in docs/design.md.
ISSUE_BODY_17
gh issue create --repo "$REPO" --title "outbox: poison records" --label "outbox" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_18'
A record that never publishes blocks its key forever. Add `WithMaxAttempts(n)` to the relay and `MarkDead` to the store: dead records stop blocking their key, are excluded from claims, and are reported through the error handler and metrics. Provide `Requeue(ids...)` to retry them manually.
ISSUE_BODY_18
gh issue create --repo "$REPO" --title "kafka: rebalance awareness" --label "kafka" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_19'
Evaluate `franz-go` (has `OnPartitionsRevoked`/`Assigned`) as an alternative adapter (`franz/` module): commit the watermark of revoked partitions and `Forget` them, stop dispatching their in-flight messages. Compare with the kafka-go adapter in docs.
ISSUE_BODY_19
gh issue create --repo "$REPO" --title "adapter: RabbitMQ" --label "enhancement" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_20'
Module `rabbitmq/` (`amqp091-go`): `Source` with manual acks and prefetch = concurrency, DLQ via dead-letter exchange or explicit publish, publisher confirms for the outbox `Publisher`.
ISSUE_BODY_20
gh issue create --repo "$REPO" --title "perf: benchmarks and load test" --label "core" --milestone "v0.2.0" --body-file - <<'ISSUE_BODY_21'
`Benchmark*` for worker throughput (concurrency 1/8/64, with/without key ordering, with/without idempotency), tracker `Done` under contention, relay flush. Publish results and the machine spec in docs/benchmarks.md; add a `-benchmem` CI job that comments regressions on PRs.
ISSUE_BODY_21

echo "Done."
