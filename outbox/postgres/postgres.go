// Package postgres is an outbox.Store backed by PostgreSQL.
//
// It uses database/sql only: bring your own driver (lib/pq, pgx stdlib, ...).
// Enqueue accepts a *sql.Tx, so the record is committed or rolled back together
// with the business change, which is the whole point of the outbox pattern.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gapfware/go-resilient-workers/outbox"
)

// ErrInvalidTable is returned when the configured table name is not a plain
// (optionally schema-qualified) identifier. The name is interpolated into SQL,
// so it is validated instead of quoted.
var ErrInvalidTable = errors.New("postgres outbox: invalid table name")

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}(\.[A-Za-z_][A-Za-z0-9_]{0,62})?$`)

// Querier is satisfied by both *sql.DB and *sql.Tx.
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store implements outbox.Store on PostgreSQL.
type Store struct {
	db    *sql.DB
	table string
}

var _ outbox.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// WithTable sets the outbox table name (default "outbox").
func WithTable(name string) Option { return func(s *Store) { s.table = name } }

// New returns a Store using db. It does not create the table; see Migrate.
func New(db *sql.DB, opts ...Option) (*Store, error) {
	s := &Store{db: db, table: "outbox"}
	for _, o := range opts {
		o(s)
	}
	if !identifier.MatchString(s.table) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTable, s.table)
	}
	return s, nil
}

// Schema returns the DDL for the outbox table and its partial indexes. The
// indexes only cover pending rows, so they stay small as published rows pile
// up until Prune removes them.
func (s *Store) Schema() string {
	idx := strings.ReplaceAll(s.table, ".", "_")
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
	id           BIGSERIAL PRIMARY KEY,
	topic        TEXT NOT NULL,
	key          BYTEA,
	payload      BYTEA,
	headers      JSONB,
	created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
	published_at TIMESTAMPTZ,
	locked_until TIMESTAMPTZ,
	attempts     INT NOT NULL DEFAULT 0,
	last_error   TEXT
);
CREATE INDEX IF NOT EXISTS %[2]s_pending_idx ON %[1]s (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS %[2]s_pending_key_idx ON %[1]s (key, id) WHERE published_at IS NULL AND key IS NOT NULL;`,
		s.table, idx)
}

// Migrate creates the table and indexes if they do not exist.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, s.Schema())
	return err
}

// Enqueue inserts rec using q, which is usually the *sql.Tx of the business
// change. An empty Key means "unordered".
func (s *Store) Enqueue(ctx context.Context, q Querier, rec outbox.Record) (string, error) {
	var headers any
	if len(rec.Headers) > 0 {
		b, err := json.Marshal(rec.Headers)
		if err != nil {
			return "", fmt.Errorf("postgres outbox: encode headers: %w", err)
		}
		headers = string(b)
	}
	var key any
	if len(rec.Key) > 0 {
		key = rec.Key
	}
	var id int64
	err := q.QueryRowContext(ctx,
		`INSERT INTO `+s.table+` (topic, key, payload, headers) VALUES ($1, $2, $3, $4::jsonb) RETURNING id`,
		rec.Topic, key, rec.Payload, headers).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("postgres outbox: enqueue: %w", err)
	}
	return strconv.FormatInt(id, 10), nil
}

// Claim implements outbox.Store.
//
// Claims are serialised with a transaction-scoped advisory lock: the "no older
// leased record of the same key" check reads rows other relays are about to
// lease, and FOR UPDATE SKIP LOCKED alone would let two relays each pick a
// different record of one key and publish them out of order. clock_timestamp()
// is used instead of now() because now() is frozen at transaction start, which
// is before a relay may have waited for the lock.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Record, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, s.table); err != nil {
		return nil, fmt.Errorf("postgres outbox: claim lock: %w", err)
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
WITH cand AS (
	SELECT o.id FROM %[1]s o
	WHERE o.published_at IS NULL
	  AND (o.locked_until IS NULL OR o.locked_until <= clock_timestamp())
	  AND NOT (o.key IS NOT NULL AND EXISTS (
		SELECT 1 FROM %[1]s b
		WHERE b.key = o.key AND b.id < o.id
		  AND b.published_at IS NULL AND b.locked_until > clock_timestamp()))
	ORDER BY o.id
	LIMIT $1
	FOR UPDATE OF o SKIP LOCKED)
UPDATE %[1]s t SET locked_until = clock_timestamp() + $2::float8 * interval '1 second'
FROM cand WHERE t.id = cand.id
RETURNING t.id, t.topic, t.key, t.payload, t.headers, t.created_at, t.attempts`, s.table),
		limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("postgres outbox: claim: %w", err)
	}
	defer rows.Close()

	var out []outbox.Record
	for rows.Next() {
		var (
			r       outbox.Record
			id      int64
			headers []byte
		)
		if err := rows.Scan(&id, &r.Topic, &r.Key, &r.Payload, &headers, &r.CreatedAt, &r.Attempts); err != nil {
			return nil, err
		}
		r.ID = strconv.FormatInt(id, 10)
		if headers != nil {
			if err := json.Unmarshal(headers, &r.Headers); err != nil {
				return nil, fmt.Errorf("postgres outbox: decode headers of %s: %w", r.ID, err)
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING has no defined order.
	sort.Slice(out, func(i, j int) bool {
		a, _ := strconv.ParseInt(out[i].ID, 10, 64)
		b, _ := strconv.ParseInt(out[j].ID, 10, 64)
		return a < b
	})
	return out, nil
}

// MarkPublished implements outbox.Store.
func (s *Store) MarkPublished(ctx context.Context, ids ...string) error {
	return s.markIDs(ctx,
		`UPDATE `+s.table+` SET published_at = clock_timestamp(), locked_until = NULL WHERE id = ANY($1::bigint[])`, ids)
}

// Release implements outbox.Store.
func (s *Store) Release(ctx context.Context, ids ...string) error {
	return s.markIDs(ctx,
		`UPDATE `+s.table+` SET locked_until = NULL WHERE id = ANY($1::bigint[]) AND published_at IS NULL`, ids)
}

// MarkFailed implements outbox.Store.
func (s *Store) MarkFailed(ctx context.Context, id string, cause error, retryIn time.Duration) error {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("postgres outbox: invalid id %q", id)
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE `+s.table+` SET attempts = attempts + 1, last_error = $2,
		 locked_until = clock_timestamp() + $3::float8 * interval '1 second' WHERE id = $1`,
		n, cause.Error(), retryIn.Seconds())
	return err
}

// Prune deletes records published more than olderThan ago and returns how many
// were removed. Run it periodically: published rows are kept for auditing and
// debugging, not for the relay.
func (s *Store) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM `+s.table+` WHERE published_at IS NOT NULL
		 AND published_at < clock_timestamp() - $1::float8 * interval '1 second'`, olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// markIDs runs query with ids as a bigint[] text literal, which every driver
// can bind without array support.
func (s *Store) markIDs(ctx context.Context, query string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for _, id := range ids {
		if _, err := strconv.ParseInt(id, 10, 64); err != nil {
			return fmt.Errorf("postgres outbox: invalid id %q", id)
		}
	}
	_, err := s.db.ExecContext(ctx, query, "{"+strings.Join(ids, ",")+"}")
	return err
}
