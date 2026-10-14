# Issues — go-resilient-workers

Create them on GitHub with `scripts/create-issues.sh` (requires `gh auth login`). The script holds the same content.
Issues labelled `starter-code` are already implemented in this repo: review against the acceptance criteria and close.

## Milestone v0.1.0

_First public release: core worker, outbox with Postgres, Kafka adapter, examples, CI, docs_

### core: retry policies

Labels: `core`, `starter-code`

## Context
Workers and the outbox relay need a shared way to decide whether to retry and how long to wait.

## Scope
- `retry.Policy` interface: `Backoff(attempt int) (time.Duration, bool)`; `attempt` = failures so far.
- `Exponential{Initial, Max, Multiplier, MaxAttempts, Jitter}` with sane defaults, overflow-safe cap, injectable `Rand` for tests.
- `Constant(delay, maxAttempts)`, `Never()`, `Default()` (5 attempts, 200ms→10s, 20% jitter).

## Acceptance criteria
- [ ] Growth, cap, max-attempts, jitter bounds and huge-attempt overflow covered by tests.
- [ ] Doc comments explain why jitter matters (thundering herd).

### core: idempotency store with leases

Labels: `core`, `starter-code`

## Context
At-least-once delivery means duplicates. Handlers must be protected without the caller writing dedup logic.

## Scope
- `idempotency.Store`: `Acquire(key, lease)`, `Complete(key, ttl)`, `Release(key)`; errors `ErrAlreadyProcessed`, `ErrInProgress`.
- `MemoryStore` (mutex + map, injectable clock, `Sweep()`).

## Acceptance criteria
- [ ] Lifecycle, release-after-failure, expired-lease takeover, TTL expiry tested.
- [ ] 64 concurrent `Acquire` on the same key → exactly one winner (`-race`).
- [ ] `Release` never forgets a completed key.

### core: worker runtime

Labels: `core`, `starter-code`

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

### core: offset tracker for contiguous commits

Labels: `core`, `kafka`, `starter-code`

## Context
With concurrency > 1, offsets finish out of order. Committing the highest finished offset can lose messages on crash.

## Scope
- `offsets.Tracker`: `Track(partition, offset)`, `Done(partition, offset) (commit int64, ok bool)`, `Pending()`, `Forget(partition)`.
- Rewind (offset <= last tracked) resets the partition.

## Acceptance criteria
- [ ] Out-of-order completion, independent partitions, rewind, unknown offsets tested.
- [ ] 2000 offsets completed concurrently in random order → final commit is the last offset, nothing pending.

### core: tests for the in-memory transport

Labels: `core`, `good first issue`

## Context
`memory.Queue` and `memory.DeadLetterQueue` are implemented but have no direct tests (they are only exercised through worker tests).

## Acceptance criteria
- [ ] `Publish` fills `ID`/`Topic` when empty and copies the message.
- [ ] `Fetch` returns `worker.ErrSourceClosed` after `Close` once the buffer is drained; respects ctx cancellation.
- [ ] `Commit` is idempotent; `Committed()` / `Uncommitted()` are accurate.
- [ ] `Publish` after `Close` returns `ErrQueueClosed`.
- [ ] `DeadLetterQueue.Entries()` returns copies.

### outbox: tests for Relay and MemoryStore

Labels: `outbox`

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

### outbox: Postgres store

Labels: `outbox`, `postgres`

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

### kafka: adapter module (Source, Publisher, DeadLetter)

Labels: `kafka`

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

### example: provisioning workflow without infrastructure

Labels: `example`

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

### example: Kafka + Postgres end-to-end with docker compose

Labels: `example`, `kafka`, `postgres`

## Scope
- `docker-compose.yml` at repo root: Kafka (apache/kafka, KRaft single node) + Postgres 16, healthchecks, topics created on start (`provisioning.requests`, `provisioning.requests.dlq`, `provisioning.events`).
- `kafka/examples/end-to-end`: an HTTP endpoint inserts a provisioning request + outbox record in one transaction; the relay publishes to Kafka; a worker consumes, calls the fake provider, emits a result event via the outbox.
- `make up`, `make demo`, `make down`.

## Acceptance criteria
- [ ] Killing the worker mid-run and restarting it loses nothing and double-applies nothing (shown in README).

### ci: GitHub Actions

Labels: `ci`

## Scope
- Workflow on push/PR: matrix Go 1.22 and stable; `gofmt` check, `go vet`, `staticcheck`, `go test -race -count=3 ./...` for root and `kafka/` modules; coverage uploaded as artifact.
- Integration job with service containers (Postgres 16, Kafka KRaft) running `-tags integration`.
- `govulncheck` job.
- Dependabot for Go modules and Actions.

## Acceptance criteria
- [ ] Badges in README (CI, Go Reference, Go Report Card, license).
- [ ] Branch protection recommendation documented in CONTRIBUTING.

### docs: README and design notes

Labels: `docs`

## Scope
- README (English): one-paragraph pitch, feature list, install, 20-line quick start, delivery contract, options table, how retries/DLQ/idempotency/ordering/shutdown interact, examples, roadmap, license.
- `docs/design.md`: message lifecycle state diagram (Mermaid), why contiguous offset commits, why the outbox relay releases instead of failing, trade-offs (head-of-line blocking with key ordering, at-least-once vs exactly-once).
- Runnable `Example*` functions in `worker` and `outbox` packages so pkg.go.dev shows them.

## Acceptance criteria
- [ ] A reader can understand the guarantees without reading code.
- [ ] Every snippet in the README compiles (covered by example tests).

### chore: repository hygiene

Labels: `docs`, `good first issue`

## Scope
- `LICENSE` (MIT), `CONTRIBUTING.md`, `CHANGELOG.md` (Keep a Changelog), issue and PR templates, `.gitignore`, `Makefile` (`test`, `test-integration`, `lint`, `up`, `down`, `demo`), `.editorconfig`.
- GitHub repo settings: description, topics (`go`, `golang`, `kafka`, `outbox-pattern`, `event-driven`, `retry`, `dead-letter-queue`, `idempotency`), website = pkg.go.dev link.

### release: v0.1.0

Labels: `docs`

## Scope
- Tag `v0.1.0` (root) and `kafka/v0.1.0`; remove the `replace` directive in `kafka/go.mod` before tagging.
- Verify both modules on pkg.go.dev; GitHub release notes from CHANGELOG.
- Pin the repo on the GitHub profile; short LinkedIn post linking to it with the design notes.

## Milestone v0.2.0

_Ecosystem: observability adapters, more stores and brokers, benchmarks_

### observability: OpenTelemetry observer and middleware

Labels: `observability`

## Scope
- Separate module `otel/`: span per message (extracting trace context from headers via a propagator), span events per retry, metrics: `messages_total{outcome}`, `attempts`, `processing_duration`, `in_flight`.
- Outbox relay metrics: pending, publish failures, lag (now - oldest pending `created_at`).

### observability: Prometheus observer

Labels: `observability`, `good first issue`

Separate module `prometheus/` exposing the same metrics as the OTel observer through `prometheus/client_golang`. Include a Grafana dashboard JSON in `docs/`.

### idempotency: Redis store

Labels: `core`, `enhancement`

Separate module `redisstore/` (`go-redis/v9`). `Acquire` with `SET key leased NX PX`, `Complete` with `SET key done PX ttl`, `Release` with a Lua compare-and-delete so a worker never releases a lease that expired and was taken over. Conformance test suite shared with `MemoryStore` (`idempotency/idempotencytest`).

### idempotency: Postgres store

Labels: `core`, `postgres`

`idempotency/postgres` using `INSERT ... ON CONFLICT` with lease expiry, so the idempotency key can be completed in the same transaction as the handler's business write (true effectively-once for DB side effects). Document this pattern in docs/design.md.

### outbox: poison records

Labels: `outbox`

A record that never publishes blocks its key forever. Add `WithMaxAttempts(n)` to the relay and `MarkDead` to the store: dead records stop blocking their key, are excluded from claims, and are reported through the error handler and metrics. Provide `Requeue(ids...)` to retry them manually.

### kafka: rebalance awareness

Labels: `kafka`

Evaluate `franz-go` (has `OnPartitionsRevoked`/`Assigned`) as an alternative adapter (`franz/` module): commit the watermark of revoked partitions and `Forget` them, stop dispatching their in-flight messages. Compare with the kafka-go adapter in docs.

### adapter: RabbitMQ

Labels: `enhancement`

Module `rabbitmq/` (`amqp091-go`): `Source` with manual acks and prefetch = concurrency, DLQ via dead-letter exchange or explicit publish, publisher confirms for the outbox `Publisher`.

### perf: benchmarks and load test

Labels: `core`

`Benchmark*` for worker throughput (concurrency 1/8/64, with/without key ordering, with/without idempotency), tracker `Done` under contention, relay flush. Publish results and the machine spec in docs/benchmarks.md; add a `-benchmem` CI job that comments regressions on PRs.
