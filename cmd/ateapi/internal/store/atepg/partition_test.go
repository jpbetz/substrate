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
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	rec := &queryRecorder{}
	pool := newMigratedPool(t, "partitioned", rec)
	if err := partitionActors(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	rec.active.Store(true)

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

	checked, fanOuts, err := findFanOuts(t.Context(), pool, rec, fanOutAllowlist)
	if err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("no statements on actors were recorded")
	}
	for _, f := range fanOuts {
		t.Error(f)
	}
}

// TestActorsPartitionableByAtespaceRejects shows what the partitioned schema rejects.
func TestActorsPartitionableByAtespaceRejects(t *testing.T) {
	t.Run("unique index omitting the key", func(t *testing.T) {
		pool := newMigratedPool(t, "partitioned-unique", &queryRecorder{})
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

	t.Run("query without the key reads every partition", func(t *testing.T) {
		rec := &queryRecorder{}
		pool := newMigratedPool(t, "partitioned-fanout", rec)
		if err := partitionActors(t.Context(), pool); err != nil {
			t.Fatal(err)
		}
		rec.active.Store(true)
		var n int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM actors WHERE uid = $1`, "u1").Scan(&n); err != nil {
			t.Fatal(err)
		}
		_, fanOuts, err := findFanOuts(t.Context(), pool, rec, fanOutAllowlist)
		if err != nil {
			t.Fatal(err)
		}
		if len(fanOuts) != 1 || !strings.Contains(fanOuts[0].sql, "WHERE uid = $1") {
			t.Fatalf("fan-outs = %v, want exactly the uid lookup", fanOuts)
		}
		t.Log(fanOuts[0])
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
// constraints and foreign keys. PostgreSQL rejects the rebuild for any index
// or foreign key that omits atespace.
func partitionActors(ctx context.Context, pool *pgxpool.Pool) error {
	// LIKE ... INCLUDING ALL copies everything but foreign keys, so those are
	// captured first and re-added after the swap.
	rows, err := pool.Query(ctx, `
		SELECT format('ALTER TABLE %s ADD CONSTRAINT %I %s', conrelid::regclass, conname, pg_get_constraintdef(oid))
		FROM pg_constraint
		WHERE contype = 'f' AND 'actors'::regclass IN (conrelid, confrelid)
		ORDER BY conname`)
	if err != nil {
		return err
	}
	addForeignKeys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	// A single Exec runs the statements in one transaction.
	_, err = pool.Exec(ctx, `
		CREATE TABLE actors_partitioned (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace);
		CREATE TABLE actors_p0 PARTITION OF actors_partitioned FOR VALUES WITH (MODULUS 2, REMAINDER 0);
		CREATE TABLE actors_p1 PARTITION OF actors_partitioned FOR VALUES WITH (MODULUS 2, REMAINDER 1);
		DROP TABLE actors CASCADE;
		ALTER TABLE actors_partitioned RENAME TO actors;
		`+strings.Join(addForeignKeys, ";\n"))
	if err != nil {
		return fmt.Errorf("actors cannot be partitioned by atespace: %w", err)
	}
	return nil
}

// queryRecorder is a pgx tracer that keeps every distinct statement the store
// issued, with the first arguments seen, for replay under EXPLAIN.
type queryRecorder struct {
	active atomic.Bool // set once the schema is partitioned
	mu     sync.Mutex
	stmts  map[string][]any
}

func (r *queryRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !r.active.Load() {
		return ctx
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stmts == nil {
		r.stmts = map[string][]any{}
	}
	if _, seen := r.stmts[data.SQL]; !seen {
		r.stmts[data.SQL] = append([]any(nil), data.Args...)
	}
	return ctx
}

func (r *queryRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

var (
	whitespaceRE = regexp.MustCompile(`\s+`)
	actorsRE     = regexp.MustCompile(`\bactors\b`)
)

func normalizeSQL(sql string) string {
	return strings.TrimSpace(whitespaceRE.ReplaceAllString(sql, " "))
}

// explainable reports whether EXPLAIN accepts the statement.
func explainable(sql string) bool {
	switch strings.ToUpper(strings.Fields(normalizeSQL(sql))[0]) {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "WITH":
		return true
	}
	return false
}

// fanOut is a recorded statement whose plan reads more than one partition.
type fanOut struct {
	sql        string
	args       []any
	partitions []string
}

func (f fanOut) String() string {
	return fmt.Sprintf("statement on actors reads partitions %v instead of one; filter on atespace or add a fanOutAllowlist entry:\n\t%s\n\targs=%v", f.partitions, normalizeSQL(f.sql), f.args)
}

// findFanOuts replays each recorded statement on actors under EXPLAIN and
// returns the ones whose plan reads more than one partition. EXPLAIN without
// ANALYZE plans but never executes, so writes are safe.
func findFanOuts(ctx context.Context, pool *pgxpool.Pool, rec *queryRecorder, allowlist map[string]string) (checked int, out []fanOut, err error) {
	rec.mu.Lock()
	var sqls []string
	for sql := range rec.stmts {
		sqls = append(sqls, sql)
	}
	rec.mu.Unlock()
	sort.Strings(sqls)

	for _, sql := range sqls {
		if !explainable(sql) || !actorsRE.MatchString(sql) {
			continue
		}
		if _, ok := allowlist[normalizeSQL(sql)]; ok {
			continue
		}
		checked++
		partitions, err := partitionsRead(ctx, pool, sql, rec.stmts[sql])
		if err != nil {
			return 0, nil, fmt.Errorf("explaining %q: %w", normalizeSQL(sql), err)
		}
		if len(partitions) > 1 {
			out = append(out, fanOut{sql: sql, args: rec.stmts[sql], partitions: partitions})
		}
	}
	return checked, out, nil
}

// partitionsRead returns the actors partitions that the plan for sql reads.
func partitionsRead(ctx context.Context, pool *pgxpool.Pool, sql string, args []any) ([]string, error) {
	var plan []byte
	if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&plan); err != nil {
		return nil, err
	}
	var doc any
	if err := json.Unmarshal(plan, &doc); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var walk func(node any)
	walk = func(node any) {
		switch n := node.(type) {
		case map[string]any:
			if name, ok := n["Relation Name"].(string); ok && strings.HasPrefix(name, "actors_p") {
				seen[name] = true
			}
			for _, child := range n {
				walk(child)
			}
		case []any:
			for _, child := range n {
				walk(child)
			}
		}
	}
	walk(doc)
	partitions := make([]string, 0, len(seen))
	for name := range seen {
		partitions = append(partitions, name)
	}
	sort.Strings(partitions)
	return partitions, nil
}

// newMigratedPool opens a pool traced by rec on a fresh schema, applies the
// migrations, and drops the schema when the test ends.
func newMigratedPool(t *testing.T, schema string, rec *queryRecorder) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()
	admin := requirePool(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+quoted+` CASCADE`) })
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = quoted
	cfg.ConnConfig.Tracer = rec
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	p, err := NewPersistence(ctx, pool)
	if err != nil {
		t.Fatalf("NewPersistence: %v", err)
	}
	p.Close()
	return pool
}
