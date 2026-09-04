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
	pool := newPartitionedPool(t, "partitioned", rec)

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
		t.Fatal("no statements on partitioned tables were recorded")
	}
	for _, f := range fanOuts {
		t.Error(f)
	}
}

// TestActorsPartitionableByAtespaceRejects shows what the partitioned schema rejects.
func TestActorsPartitionableByAtespaceRejects(t *testing.T) {
	t.Run("unique index omitting the key", func(t *testing.T) {
		pool := newTracedPool(t, "partitioned-unique", &queryRecorder{})
		migrate(t, pool)
		// This also rules out foreign keys onto uid, which need this index.
		if _, err := pool.Exec(t.Context(), `CREATE UNIQUE INDEX actors_uid_key ON actors (uid)`); err != nil {
			t.Fatal(err)
		}
		err := partitionTable(t.Context(), pool, "actors", "atespace")
		if err == nil || !strings.Contains(err.Error(), "must include all partitioning columns") {
			t.Fatalf("partitionTable error = %v, want unique index rejection", err)
		}
		t.Log(err)
	})

	t.Run("query without the key reads every partition", func(t *testing.T) {
		rec := &queryRecorder{}
		pool := newPartitionedPool(t, "partitioned-fanout", rec)
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

// partitionKeys maps each table the test partitions to its partition column.
var partitionKeys = map[string]string{
	"actors": "atespace",
}

// Two partitions are enough to expose every partitioning constraint.
const partitionCount = 2

// fanOutAllowlist holds the statements allowed to read more than one
// partition, keyed by whitespace-normalized SQL.
var fanOutAllowlist = map[string]string{
	normalizeSQL(`
		SELECT atespace, name, proto FROM actors
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`): "global ListActors is a keyset scan over every atespace",
}

// partitionTable rebuilds table as a hash-partitioned table on key, keeping
// its rows, indexes, constraints and foreign keys. PostgreSQL rejects the
// rebuild for any index or foreign key that omits key.
func partitionTable(ctx context.Context, pool *pgxpool.Pool, table, key string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	type fk struct{ table, name, def string }
	var fks []fk
	rows, err := tx.Query(ctx, `
		SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)
		FROM pg_constraint
		WHERE contype = 'f' AND (conrelid = $1::regclass OR confrelid = $1::regclass)
		ORDER BY conname`, table)
	if err != nil {
		return fmt.Errorf("listing foreign keys of %s: %w", table, err)
	}
	for rows.Next() {
		var f fk
		if err := rows.Scan(&f.table, &f.name, &f.def); err != nil {
			rows.Close()
			return err
		}
		fks = append(fks, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	q := func(s string) string { return pgx.Identifier{s}.Sanitize() }
	tmp := table + "_partitioned"
	// LIKE ... INCLUDING ALL replays every index and constraint the migrations
	// created. Foreign keys are not copied and are re-added below.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (LIKE %s INCLUDING ALL) PARTITION BY HASH (%s)`, q(tmp), q(table), q(key))); err != nil {
		return fmt.Errorf("table %s cannot be partitioned by %s: %w", table, key, err)
	}
	for i := 0; i < partitionCount; i++ {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d)`, q(fmt.Sprintf("%s_p%d", table, i)), q(tmp), partitionCount, i)); err != nil {
			return fmt.Errorf("creating partition %d of %s: %w", i, table, err)
		}
	}
	for _, f := range fks {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s`, f.table, q(f.name))); err != nil {
			return fmt.Errorf("dropping foreign key %s: %w", f.name, err)
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s SELECT * FROM %s`, q(tmp), q(table))); err != nil {
		return fmt.Errorf("copying %s rows into partitions: %w", table, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, q(table))); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, q(tmp), q(table))); err != nil {
		return err
	}
	for _, f := range fks {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s %s`, f.table, q(f.name), f.def)); err != nil {
			return fmt.Errorf("foreign key %s on %s cannot reference %s partitioned by %s: %w", f.name, f.table, table, key, err)
		}
	}
	return tx.Commit(ctx)
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
	literalRE    = regexp.MustCompile(`'[^']*'`)
)

func normalizeSQL(sql string) string {
	return strings.TrimSpace(whitespaceRE.ReplaceAllString(sql, " "))
}

// mentionsTable reports whether sql names table outside string literals.
func mentionsTable(sql, table string) bool {
	stripped := literalRE.ReplaceAllString(sql, "''")
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(table) + `\b`).MatchString(stripped)
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
	table      string
	sql        string
	args       []any
	partitions []string
}

func (f fanOut) String() string {
	return fmt.Sprintf("statement on %s reads partitions %v instead of one; filter on %s or add a fanOutAllowlist entry:\n\t%s\n\targs=%v", f.table, f.partitions, partitionKeys[f.table], normalizeSQL(f.sql), f.args)
}

// findFanOuts replays each recorded statement on a partitioned table under
// EXPLAIN and returns the ones whose plan reads more than one partition.
// EXPLAIN without ANALYZE plans but never executes, so writes are safe.
func findFanOuts(ctx context.Context, pool *pgxpool.Pool, rec *queryRecorder, allowlist map[string]string) (checked int, out []fanOut, err error) {
	rec.mu.Lock()
	var sqls []string
	for sql := range rec.stmts {
		sqls = append(sqls, sql)
	}
	rec.mu.Unlock()
	sort.Strings(sqls)

	for _, sql := range sqls {
		if !explainable(sql) {
			continue
		}
		if _, ok := allowlist[normalizeSQL(sql)]; ok {
			continue
		}
		for table := range partitionKeys {
			if !mentionsTable(sql, table) {
				continue
			}
			checked++
			partitions, err := partitionsRead(ctx, pool, sql, rec.stmts[sql], table)
			if err != nil {
				return 0, nil, fmt.Errorf("explaining %q: %w", normalizeSQL(sql), err)
			}
			if len(partitions) > 1 {
				out = append(out, fanOut{table: table, sql: sql, args: rec.stmts[sql], partitions: partitions})
			}
		}
	}
	return checked, out, nil
}

// partitionsRead returns the partitions of table that the plan for sql reads.
func partitionsRead(ctx context.Context, pool *pgxpool.Pool, sql string, args []any, table string) ([]string, error) {
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
			if name, ok := n["Relation Name"].(string); ok && strings.HasPrefix(name, table+"_p") {
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

// newPartitionedPool opens a traced pool on a fresh schema, applies the
// migrations, and partitions the tables in partitionKeys. The schema is
// dropped when the test ends.
func newPartitionedPool(t *testing.T, schema string, rec *queryRecorder) *pgxpool.Pool {
	t.Helper()
	pool := newTracedPool(t, schema, rec)
	migrate(t, pool)
	for table, key := range partitionKeys {
		if err := partitionTable(t.Context(), pool, table, key); err != nil {
			t.Fatalf("partitioning %s: %v", table, err)
		}
	}
	rec.active.Store(true)
	return pool
}

func newTracedPool(t *testing.T, schema string, rec *queryRecorder) *pgxpool.Pool {
	t.Helper()
	admin := requirePool(t)
	ctx := t.Context()
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("creating schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`)
	})
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = quoted
	cfg.ConnConfig.Tracer = rec
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func migrate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	p, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatalf("NewPersistence: %v", err)
	}
	p.Close()
}
