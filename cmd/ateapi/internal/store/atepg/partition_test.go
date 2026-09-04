// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package atepg

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storecontract"
)

// TestActorsPartitionableByAtespace exists to keep it possible to partition the actors table by
// atespace later. It runs the store contract suite against a copy of the
// schema in which actors is hash-partitioned by atespace, and fails on any
// schema change or query that would not work in that layout.
func TestActorsPartitionableByAtespace(t *testing.T) {
	pool, checker := partitionedPool(t, "partitioned", t.Error)
	storecontract.RunContractTests(t, func(t *testing.T) store.Interface {
		t.Helper()
		p, err := NewPersistence(t.Context(), pool)
		if err != nil {
			t.Fatalf("NewPersistence: %v", err)
		}
		t.Cleanup(p.Close)
		clearAll(t, p)
		return p
	})
	if len(checker.seen) == 0 {
		t.Fatal("no statements on actors were checked")
	}
}

// TestActorsPartitionableByAtespaceRejects shows what the partitioned schema rejects.
func TestActorsPartitionableByAtespaceRejects(t *testing.T) {
	t.Run("unique index omitting atespace", func(t *testing.T) {
		pool := migratedPool(t, "partitioned-unique")
		// This also rules out foreign keys onto uid, which need this index.
		if _, err := pool.Exec(t.Context(), `CREATE UNIQUE INDEX actors_uid_key ON actors (uid)`); err != nil {
			t.Fatal(err)
		}
		err := partitionActors(t.Context(), pool)
		if err == nil || !strings.Contains(err.Error(), "must include all partitioning columns") {
			t.Fatalf("partitionActors error = %v, want unique index rejection", err)
		}
		t.Log(err)
	})

	t.Run("query without atespace reads every partition", func(t *testing.T) {
		var got []any
		pool, _ := partitionedPool(t, "partitioned-fanout", func(args ...any) { got = append(got, args...) })
		var n int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM actors WHERE uid = $1`, "u1").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || !strings.Contains(fmt.Sprint(got[0]), "WHERE uid = $1") {
			t.Fatalf("reported = %v, want exactly the uid lookup", got)
		}
		t.Log(got[0])
	})
}

// fanOutAllowlist holds the statements allowed to read more than one
// partition, keyed by whitespace-normalized SQL.
var fanOutAllowlist = map[string]string{
	normalizeSQL(`
		SELECT atespace, name, proto FROM actors
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`): "global ListActors is a keyset scan over every atespace",
}

func normalizeSQL(sql string) string { return strings.Join(strings.Fields(sql), " ") }

// partitionActors rebuilds actors as a two-way hash-partitioned table on
// atespace, keeping its rows, indexes, constraints and foreign keys.
// PostgreSQL rejects the rebuild for any index or foreign key that omits
// atespace.
func partitionActors(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// LIKE ... INCLUDING ALL replays every index and constraint the migrations
	// created. Foreign keys are not copied, so they are dropped and re-added.
	rows, err := tx.Query(ctx, `
		SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid) FROM pg_constraint
		WHERE contype = 'f' AND (conrelid = 'actors'::regclass OR confrelid = 'actors'::regclass)`)
	if err != nil {
		return err
	}
	fks, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct{ Table, Name, Def string }])
	if err != nil {
		return err
	}
	steps := []string{
		`CREATE TABLE actors_partitioned (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace)`,
		`CREATE TABLE actors_p0 PARTITION OF actors_partitioned FOR VALUES WITH (MODULUS 2, REMAINDER 0)`,
		`CREATE TABLE actors_p1 PARTITION OF actors_partitioned FOR VALUES WITH (MODULUS 2, REMAINDER 1)`,
	}
	for _, fk := range fks {
		steps = append(steps, fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s`, fk.Table, fk.Name))
	}
	steps = append(steps,
		`INSERT INTO actors_partitioned SELECT * FROM actors`,
		`DROP TABLE actors`,
		`ALTER TABLE actors_partitioned RENAME TO actors`,
	)
	for _, fk := range fks {
		steps = append(steps, fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s %s`, fk.Table, fk.Name, fk.Def))
	}
	for _, step := range steps {
		if _, err := tx.Exec(ctx, step); err != nil {
			return fmt.Errorf("actors cannot be partitioned by atespace: %w (while running %s)", err, step)
		}
	}
	return tx.Commit(ctx)
}

// fanOutChecker is a pgx tracer that explains each new statement on actors
// and reports the ones whose plan reads more than one partition.
type fanOutChecker struct {
	explain *pgxpool.Pool // untraced, so the check never runs inside the caller's transaction
	report  func(args ...any)
	mu      sync.Mutex
	seen    map[string]bool
}

var (
	dmlRE       = regexp.MustCompile(`(?is)^\s*(select|insert|update|delete)\b`)
	partitionRE = regexp.MustCompile(`\bactors_p\d+\b`)
)

func (c *fanOutChecker) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	sql := normalizeSQL(data.SQL)
	if !strings.Contains(sql, "actors") || !dmlRE.MatchString(sql) || fanOutAllowlist[sql] != "" {
		return ctx
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen[sql] {
		return ctx
	}
	c.seen[sql] = true

	// EXPLAIN without ANALYZE plans but never executes, so writes are safe.
	rows, err := c.explain.Query(ctx, "EXPLAIN "+data.SQL, data.Args...)
	if err != nil {
		c.report(fmt.Errorf("explaining %q: %w", sql, err))
		return ctx
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		c.report(fmt.Errorf("explaining %q: %w", sql, err))
		return ctx
	}
	partitions := partitionRE.FindAllString(strings.Join(lines, "\n"), -1)
	slices.Sort(partitions)
	if partitions = slices.Compact(partitions); len(partitions) > 1 {
		c.report(fmt.Sprintf("statement on actors reads partitions %v instead of one; filter on atespace or add it to fanOutAllowlist:\n\t%s\n\targs=%v", partitions, sql, data.Args))
	}
	return ctx
}

func (c *fanOutChecker) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// partitionedPool returns a pool on a fresh, migrated schema whose actors
// table is partitioned, with every statement checked by a fanOutChecker.
func partitionedPool(t *testing.T, schema string, report func(args ...any)) (*pgxpool.Pool, *fanOutChecker) {
	t.Helper()
	plain := migratedPool(t, schema)
	if err := partitionActors(t.Context(), plain); err != nil {
		t.Fatal(err)
	}
	checker := &fanOutChecker{explain: plain, report: report, seen: map[string]bool{}}
	return newPool(t, schema, checker), checker
}

// migratedPool returns a pool on a fresh schema with the migrations applied.
// The schema is dropped when the test ends.
func migratedPool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	admin := requirePool(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("creating schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`)
	})
	pool := newPool(t, schema, nil)
	p, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatalf("NewPersistence: %v", err)
	}
	p.Close()
	return pool
}

func newPool(t *testing.T, schema string, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	if tracer != nil {
		cfg.ConnConfig.Tracer = tracer
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
