# go-resilient-workers — project brief

## Goal

A public, production-grade Go library for running message handlers reliably on
top of at-least-once brokers (Kafka first). It is the open-source version of
patterns the author built in production (Kafka provisioning workers at a
telecom, content-ingestion pipelines at a streaming platform) and must read as
a portfolio piece for international backend roles: small, correct, well
tested, well documented. No employer code or data, ever.

What a user gets:

- A `worker` runtime: retries with backoff, permanent vs retryable errors,
  dead-letter queue, idempotency, per-key ordering, panic recovery, handler
  timeouts, graceful shutdown, observability hooks.
- A transactional `outbox` with a Postgres store and a relay that preserves
  per-key order.
- A `kafka` adapter that commits only contiguous offsets when processing
  concurrently.

## Non-negotiable design rules

1. **Delivery contract.** A message is committed to its source only after it
   was (a) handled successfully, (b) skipped as a duplicate, or (c) durably
   accepted by the dead-letter destination. Anything else — crash, shutdown
   during a backoff wait, DLQ outage — leaves it uncommitted for redelivery.
2. **Zero-dependency core.** The root module (`retry`, `idempotency`,
   `worker`, `memory`, `offsets`, `outbox`) uses only the standard library.
   Test-only deps are allowed (`github.com/lib/pq` for Postgres integration
   tests). Broker adapters live in their own Go modules (`kafka/go.mod`) so
   users of the core never pull them.
3. **Shutdown semantics.** Cancelling `Run`'s context stops fetching at once.
   In-flight handlers keep a context that survives the cancellation until
   they finish or `WithShutdownTimeout` elapses. Messages waiting for a retry
   are abandoned (uncommitted), so shutdown never waits for a long backoff.
4. **Idempotency.** Acquire a lease before the handler, Complete on success,
   Release on failure. A failure to record Complete is reported, never turned
   into a retry (the side effect already happened).
5. **Kafka offsets.** With concurrency > 1 messages finish out of order;
   `offsets.Tracker` only advances the commit point over a contiguous run of
   finished offsets, and resets a partition when offsets rewind (rebalance).
6. **Outbox ordering.** `Store.Claim` never returns a record while an older
   unpublished record with the same non-empty key is leased or backing off.
   The relay releases (does not fail) the rest of a key's batch after a
   failure. The Postgres store serialises claims with
   `pg_advisory_xact_lock` so multiple relays cannot reorder a key.

## Layout

```
retry/         backoff policies (Exponential w/ jitter, Constant, Never, Default)
idempotency/   Store interface + MemoryStore (lease / complete / release, Sweep)
worker/        Worker runtime, options, errors (Permanent, RetryAfter, PanicError), Observer, Stats, SlogObserver
memory/        in-memory Queue (Source) and DeadLetterQueue for tests/examples
offsets/       Tracker for contiguous commits on partitioned logs
outbox/        Record/Store/Publisher, Relay, MemoryStore; outbox/postgres (TODO)
kafka/         separate module: Source, Publisher, DeadLetter (TODO)
examples/      provisioning (no infra), kafka-postgres end-to-end (TODO)
```

## Current status

Implemented and passing `go test -race ./...`: `retry`, `idempotency`,
`worker`, `offsets`, `outbox` (interfaces, `Relay`, `MemoryStore`), `memory`
(no tests yet). Everything else is open in the
issues (see ISSUES.md / GitHub issues). Issues labelled `starter-code` are
already implemented: review, fill gaps against the acceptance criteria, close.

## Conventions

- Go 1.22 (`go.mod`), uses `math/rand/v2`, `log/slog`, `context.WithoutCancel`.
- Every exported identifier has a doc comment explaining *why*, not just what.
- Errors: wrap with `%w`; sentinel errors are exported `Err...` vars.
- Tests: table-driven where it helps, no sleeps longer than tens of ms,
  everything green under `-race`. Integration tests use the build tag
  `integration` and env vars (`DATABASE_URL`, `KAFKA_BROKERS`).
- Lint: `go vet` + `staticcheck` clean; `gofmt` enforced in CI.
- Public docs, README, issues and commit messages in English.
- Conventional commits (`feat(worker): ...`, `fix(outbox): ...`, `docs: ...`).

## Definition of done (per issue)

Code + tests + doc comments, `go vet` / `staticcheck` / `go test -race` green,
README or docs updated when behaviour is user-visible.

## Commands

```bash
go test -race ./...                                   # core
DATABASE_URL=postgres://postgres@localhost:5432/postgres?sslmode=disable \
  go test -race -tags integration ./outbox/...        # Postgres store
cd kafka && KAFKA_BROKERS=localhost:9092 go test -race -tags integration ./...
docker compose up -d                                  # Kafka (KRaft) + Postgres for local runs
```
