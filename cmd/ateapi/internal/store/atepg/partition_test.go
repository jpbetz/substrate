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
	pool := partitionedPool(t, "partitioned")
	check := &fanOutCheck{explain: pool, fail: t.Errorf, seen: map[string]bool{}}
	traced := openPool(t, "partitioned", check)
	storecontract.RunContractTests(t, func(t *testing.T) store.Interface {
		p, err := NewPersistence(t.Context(), traced)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		clearAll(t, p)
		return p
	})
	if len(check.seen) == 0 {
		t.Fatal("no statements on actors were traced")
	}
}

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
		var got string
		pool := partitionedPool(t, "partitioned-fanout")
		check := &fanOutCheck{explain: pool, seen: map[string]bool{}, fail: func(format string, args ...any) { got = fmt.Sprintf(format, args...) }}
		var n int
		if err := openPool(t, "partitioned-fanout", check).QueryRow(t.Context(), `SELECT count(*) FROM actors WHERE uid = $1`, "u1").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, "[actors_p0 actors_p1]") {
			t.Fatalf("fan-out check reported %q, want the uid lookup reading both partitions", got)
		}
		t.Log(got)
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

// partitionActors rebuilds the empty, freshly migrated actors table as a
// two-way hash-partitioned table on atespace, keeping its indexes,
// constraints and foreign keys. PostgreSQL rejects any of them that omits
// atespace.
func partitionActors(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT format('ALTER TABLE %s ADD CONSTRAINT %I %s', conrelid::regclass, conname, pg_get_constraintdef(oid))
		FROM pg_constraint
		WHERE contype = 'f' AND 'actors'::regclass IN (conrelid, confrelid)`)
	if err != nil {
		return err
	}
	foreignKeys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE actors_partitioned (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace);
		CREATE TABLE actors_p0 PARTITION OF actors_partitioned FOR VALUES WITH (MODULUS 2, REMAINDER 0);
		CREATE TABLE actors_p1 PARTITION OF actors_partitioned FOR VALUES WITH (MODULUS 2, REMAINDER 1);
		DROP TABLE actors CASCADE;
		ALTER TABLE actors_partitioned RENAME TO actors`); err != nil {
		return fmt.Errorf("actors cannot be partitioned by atespace: %w", err)
	}
	for _, fk := range foreignKeys {
		if _, err := pool.Exec(ctx, fk); err != nil {
			return fmt.Errorf("foreign key cannot reference actors partitioned by atespace: %w", err)
		}
	}
	return nil
}

// fanOutCheck is a pgx tracer that explains each distinct statement on
// actors and fails when its plan reads more than one partition.
type fanOutCheck struct {
	explain *pgxpool.Pool
	fail    func(format string, args ...any)
	mu      sync.Mutex
	seen    map[string]bool
}

var partitionRE = regexp.MustCompile(`\bactors_p\d+\b`)

func (c *fanOutCheck) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	sql := normalizeSQL(data.SQL)
	verb, _, _ := strings.Cut(strings.ToUpper(sql), " ")
	if !strings.Contains(sql, "actors") || !strings.Contains("SELECT INSERT UPDATE DELETE", verb) {
		return ctx
	}
	if _, ok := fanOutAllowlist[sql]; ok {
		return ctx
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen[sql] {
		return ctx
	}
	c.seen[sql] = true
	// EXPLAIN without ANALYZE plans but never executes, so writes are safe.
	rows, _ := c.explain.Query(ctx, "EXPLAIN "+data.SQL, data.Args...)
	plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		c.fail("explaining %s: %v", sql, err)
		return ctx
	}
	partitions := slices.Compact(slices.Sorted(slices.Values(partitionRE.FindAllString(strings.Join(plan, "\n"), -1))))
	if len(partitions) > 1 {
		c.fail("statement on actors reads partitions %v instead of one; filter on atespace or add a fanOutAllowlist entry:\n\t%s\n\targs=%v", partitions, sql, data.Args)
	}
	return ctx
}

func (c *fanOutCheck) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func normalizeSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// migratedPool opens a pool on a fresh schema with the migrations applied.
func migratedPool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	admin := requirePool(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`) })
	pool := openPool(t, schema, nil)
	p, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	return pool
}

func partitionedPool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	pool := migratedPool(t, schema)
	if err := partitionActors(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// openPool opens a pool on schema, tracing every statement with tracer.
func openPool(t *testing.T, schema string, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
