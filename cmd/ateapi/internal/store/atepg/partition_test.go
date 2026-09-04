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

// Partition mode keeps the production schema partition-ready without shipping
// partitions. It rebuilds the tables in partitionKeys as HASH-partitioned
// tables from the live catalog, so whatever the migrations created is what
// gets partitioned, then runs the store contract suite against them and
// checks that every statement touching a partitioned table prunes to one
// partition. Anything that would make partitioning harder later fails here:
// a unique index that omits the key, a foreign key referencing columns that
// omit it, a query that fans out across partitions, or an update that moves
// rows between them.

import (
	"context"
	"encoding/json"
	"errors"
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

// partitionKeys maps each table partition mode converts to the column it is
// partitioned on. actor_templates, actor_snapshots, actor_snapshot_tags and
// actor_egress_policies are candidates for the same treatment on atespace.
var partitionKeys = map[string]string{
	"actors": "atespace",
}

// partitionCount is deliberately small: two partitions are enough to expose
// every partitioning constraint, and hash routing spreads test data over both.
const partitionCount = 2

// fanOutAllowlist lists the statements allowed to read more than one
// partition, keyed by whitespace-normalized SQL. Add to it deliberately: each
// entry is a query that will scan every partition once the table is
// partitioned for real.
var fanOutAllowlist = map[string]string{
	normalizeSQL(`
		SELECT atespace, name, proto FROM actors
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`): "global ListActors is a keyset scan over every atespace",
}

// partitionForTest rebuilds table as a HASH-partitioned table on key with
// the given number of partitions, preserving its rows, indexes, constraints
// and the foreign keys in both directions, and installs a trigger that
// rejects updates to key. It returns a descriptive error for every schema
// property that blocks partitioning.
func partitionForTest(ctx context.Context, pool *pgxpool.Pool, table, key string, partitions int) error {
	if err := checkPartitionable(ctx, pool, table, key); err != nil {
		return err
	}
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
	// created, so PostgreSQL itself rejects anything a partitioned table
	// cannot carry.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (LIKE %s INCLUDING ALL) PARTITION BY HASH (%s)`, q(tmp), q(table), q(key))); err != nil {
		return fmt.Errorf("table %s cannot be partitioned by %s: %w", table, key, err)
	}
	for i := 0; i < partitions; i++ {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d)`, q(partitionName(table, i)), q(tmp), partitions, i)); err != nil {
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
			return fmt.Errorf("foreign key %s on %s cannot reference partitioned %s: %w", f.name, f.table, table, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		CREATE OR REPLACE FUNCTION partition_key_guard() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'partition mode: UPDATE of %.% moves rows between partitions', TG_ARGV[0], TG_ARGV[1];
		END $$`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE UPDATE OF %s ON %s FOR EACH ROW
		WHEN (OLD.%s IS DISTINCT FROM NEW.%s)
		EXECUTE FUNCTION partition_key_guard(%s, %s)`,
		q(table+"_partition_key_guard"), q(key), q(table), q(key), q(key), quoteLiteral(table), quoteLiteral(key))); err != nil {
		return fmt.Errorf("installing partition key guard on %s: %w", table, err)
	}
	return tx.Commit(ctx)
}

func partitionName(table string, i int) string { return fmt.Sprintf("%s_p%d", table, i) }

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// checkPartitionable reports, by name, every unique index on table and every
// foreign key referencing table whose columns omit key. PostgreSQL would
// reject the same things, but one message per offender names the culprit.
func checkPartitionable(ctx context.Context, pool *pgxpool.Pool, table, key string) error {
	var errs []error
	rows, err := pool.Query(ctx, `
		SELECT c.relname, array_agg(a.attname::text ORDER BY k.ord)
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
		WHERE i.indrelid = $1::regclass AND i.indisunique
		GROUP BY c.relname ORDER BY c.relname`, table)
	if err != nil {
		return fmt.Errorf("listing unique indexes of %s: %w", table, err)
	}
	for rows.Next() {
		var name string
		var cols []string
		if err := rows.Scan(&name, &cols); err != nil {
			rows.Close()
			return err
		}
		if !contains(cols, key) {
			errs = append(errs, fmt.Errorf("unique index %s on %s(%s) omits partition key %s: a partitioned table cannot enforce it", name, table, strings.Join(cols, ", "), key))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = pool.Query(ctx, `
		SELECT conname, conrelid::regclass::text,
			(SELECT array_agg(a.attname::text ORDER BY k.ord)
			 FROM unnest(confkey) WITH ORDINALITY AS k(attnum, ord)
			 JOIN pg_attribute a ON a.attrelid = confrelid AND a.attnum = k.attnum)
		FROM pg_constraint
		WHERE contype = 'f' AND confrelid = $1::regclass
		ORDER BY conname`, table)
	if err != nil {
		return fmt.Errorf("listing foreign keys referencing %s: %w", table, err)
	}
	for rows.Next() {
		var name, from string
		var cols []string
		if err := rows.Scan(&name, &from, &cols); err != nil {
			rows.Close()
			return err
		}
		if !contains(cols, key) {
			errs = append(errs, fmt.Errorf("foreign key %s on %s references %s(%s) without partition key %s: a partitioned table cannot be referenced that way", name, from, table, strings.Join(cols, ", "), key))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	return errors.Join(errs...)
}

func contains(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// queryRecorder is a pgx tracer that keeps every distinct statement the
// store issued, with a few sample argument sets each, so the statements can
// be replayed under EXPLAIN after the suite ran.
type queryRecorder struct {
	active atomic.Bool // set once the schema is partitioned; earlier statements are setup noise
	mu     sync.Mutex
	stmts  map[string][][]any
}

const argSamplesPerStatement = 4

func (r *queryRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !r.active.Load() {
		return ctx
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stmts == nil {
		r.stmts = map[string][][]any{}
	}
	if samples := r.stmts[data.SQL]; len(samples) < argSamplesPerStatement {
		r.stmts[data.SQL] = append(samples, append([]any(nil), data.Args...))
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

// mentionsTable reports whether sql references table outside string literals.
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

// explained is a recorded statement on a partitioned table together with
// the partitions its plan reads.
type explained struct {
	table      string
	sql        string
	args       []any
	partitions []string
}

func (e explained) fansOut() bool { return len(e.partitions) > 1 }

func (e explained) String() string {
	return fmt.Sprintf("statement on %s touches partitions %v instead of one; filter on %s or add an explicit fanOutAllowlist entry:\n\t%s\n\targs=%v", e.table, e.partitions, partitionKeys[e.table], normalizeSQL(e.sql), e.args)
}

// explainRecorded replays each recorded statement that names a partitioned
// table under EXPLAIN, skipping allowlisted ones, and reports the partitions
// each plan reads. A statement with several argument samples is reported by
// the widest plan.
func explainRecorded(ctx context.Context, pool *pgxpool.Pool, rec *queryRecorder, allowlist map[string]string) ([]explained, error) {
	rec.mu.Lock()
	stmts := make(map[string][][]any, len(rec.stmts))
	for sql, samples := range rec.stmts {
		stmts[sql] = samples
	}
	rec.mu.Unlock()

	var sqls []string
	for sql := range stmts {
		sqls = append(sqls, sql)
	}
	sort.Strings(sqls)

	var out []explained
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
			widest := explained{table: table, sql: sql}
			for _, args := range stmts[sql] {
				partitions, err := partitionsScanned(ctx, pool, sql, args, table)
				if err != nil {
					return nil, fmt.Errorf("explaining %q: %w", normalizeSQL(sql), err)
				}
				if len(partitions) >= len(widest.partitions) {
					widest.args, widest.partitions = args, partitions
				}
			}
			out = append(out, widest)
		}
	}
	return out, nil
}

// partitionsScanned returns the distinct partitions of table that the plan
// for sql with args reads. EXPLAIN without ANALYZE plans but never executes,
// so writes are safe to replay.
func partitionsScanned(ctx context.Context, pool *pgxpool.Pool, sql string, args []any, table string) ([]string, error) {
	var plan []byte
	if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&plan); err != nil {
		return nil, err
	}
	var doc any
	if err := json.Unmarshal(plan, &doc); err != nil {
		return nil, err
	}
	prefix := table + "_p"
	seen := map[string]bool{}
	var walk func(node any)
	walk = func(node any) {
		switch n := node.(type) {
		case map[string]any:
			if name, ok := n["Relation Name"].(string); ok && strings.HasPrefix(name, prefix) {
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

// newPartitionModePool opens a pool on a fresh schema with rec tracing every
// statement. The schema is dropped when the test ends.
func newPartitionModePool(t *testing.T, schema string, rec *queryRecorder) *pgxpool.Pool {
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
		t.Fatalf("opening partition mode pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// migrateAndPartition applies the migrations, converts every table in
// partitionKeys, and then starts recording statements.
func migrateAndPartition(t *testing.T, pool *pgxpool.Pool, rec *queryRecorder) {
	t.Helper()
	ctx := t.Context()
	p, err := NewPersistence(ctx, pool)
	if err != nil {
		t.Fatalf("NewPersistence: %v", err)
	}
	p.Close()
	for table, key := range partitionKeys {
		if err := partitionForTest(ctx, pool, table, key, partitionCount); err != nil {
			t.Fatalf("partition mode: %v", err)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != partitionCount {
			t.Fatalf("%s has %d partitions, want %d", table, n, partitionCount)
		}
	}
	rec.active.Store(true)
}

// TestPartitionMode runs the store contract suite against a schema whose
// partitionKeys tables are hash-partitioned, then verifies every statement
// the suite issued against them prunes to a single partition.
func TestPartitionMode(t *testing.T) {
	rec := &queryRecorder{}
	pool := newPartitionModePool(t, "partition-mode", rec)
	migrateAndPartition(t, pool, rec)

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

	results, err := explainRecorded(t.Context(), pool, rec, fanOutAllowlist)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("no statements on partitioned tables were recorded; the tracer is not wired")
	}
	for _, r := range results {
		if r.fansOut() {
			t.Error(r)
			continue
		}
		t.Logf("%v %s", r.partitions, normalizeSQL(r.sql))
	}
}

// TestPartitionModeGuards proves each guard fires with a readable message.
func TestPartitionModeGuards(t *testing.T) {
	t.Run("unique index omitting the key", func(t *testing.T) {
		pool := newPartitionModePool(t, "partition-guard-unique", &queryRecorder{})
		p, err := NewPersistence(t.Context(), pool)
		if err != nil {
			t.Fatal(err)
		}
		p.Close()
		if _, err := pool.Exec(t.Context(), `CREATE UNIQUE INDEX actors_uid_key ON actors (uid)`); err != nil {
			t.Fatal(err)
		}
		err = partitionForTest(t.Context(), pool, "actors", "atespace", partitionCount)
		if err == nil || !strings.Contains(err.Error(), "unique index actors_uid_key on actors(uid) omits partition key atespace") {
			t.Fatalf("partitionForTest error = %v, want unique index violation", err)
		}
		t.Log(err)
	})

	t.Run("foreign key omitting the key", func(t *testing.T) {
		pool := newPartitionModePool(t, "partition-guard-fk", &queryRecorder{})
		p, err := NewPersistence(t.Context(), pool)
		if err != nil {
			t.Fatal(err)
		}
		p.Close()
		if _, err := pool.Exec(t.Context(), `
			CREATE UNIQUE INDEX actors_uid_key ON actors (uid);
			CREATE TABLE actor_notes (actor_uid text NOT NULL CONSTRAINT actor_notes_actor_fk REFERENCES actors (uid))`); err != nil {
			t.Fatal(err)
		}
		err = partitionForTest(t.Context(), pool, "actors", "atespace", partitionCount)
		if err == nil || !strings.Contains(err.Error(), "foreign key actor_notes_actor_fk on actor_notes references actors(uid) without partition key atespace") {
			t.Fatalf("partitionForTest error = %v, want foreign key violation", err)
		}
		t.Log(err)
	})

	t.Run("PostgreSQL rejects what the precheck misses", func(t *testing.T) {
		// An exclusion constraint is not a unique index, so only the LIKE
		// replay catches it.
		pool := newPartitionModePool(t, "partition-guard-exclude", &queryRecorder{})
		p, err := NewPersistence(t.Context(), pool)
		if err != nil {
			t.Fatal(err)
		}
		p.Close()
		if _, err := pool.Exec(t.Context(), `ALTER TABLE actors ADD CONSTRAINT actors_uid_excl EXCLUDE (uid WITH =)`); err != nil {
			t.Fatal(err)
		}
		err = partitionForTest(t.Context(), pool, "actors", "atespace", partitionCount)
		if err == nil || !strings.Contains(err.Error(), "table actors cannot be partitioned by atespace") {
			t.Fatalf("partitionForTest error = %v, want PostgreSQL rejection", err)
		}
		t.Log(err)
	})

	t.Run("query without the key fans out", func(t *testing.T) {
		rec := &queryRecorder{}
		pool := newPartitionModePool(t, "partition-guard-fanout", rec)
		migrateAndPartition(t, pool, rec)
		var n int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM actors WHERE uid = $1`, "u1").Scan(&n); err != nil {
			t.Fatal(err)
		}
		results, err := explainRecorded(t.Context(), pool, rec, fanOutAllowlist)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || !results[0].fansOut() || !strings.Contains(results[0].sql, "WHERE uid = $1") {
			t.Fatalf("explained = %v, want exactly the uid lookup fanning out", results)
		}
		t.Log(results[0])
	})

	t.Run("global list fans out unless allowlisted", func(t *testing.T) {
		rec := &queryRecorder{}
		pool := newPartitionModePool(t, "partition-guard-allowlist", rec)
		migrateAndPartition(t, pool, rec)
		p, err := NewPersistence(t.Context(), pool)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		if _, err := p.ListActors(t.Context(), "", store.ListOptions{PageSize: 10}); err != nil {
			t.Fatal(err)
		}
		results, err := explainRecorded(t.Context(), pool, rec, nil)
		if err != nil {
			t.Fatal(err)
		}
		var flagged []explained
		for _, r := range results {
			if r.fansOut() {
				flagged = append(flagged, r)
			}
		}
		if len(flagged) != 1 || !strings.Contains(flagged[0].sql, "ORDER BY atespace, name") {
			t.Fatalf("flagged = %v, want exactly the global ListActors scan", flagged)
		}
		if results, err = explainRecorded(t.Context(), pool, rec, fanOutAllowlist); err != nil {
			t.Fatal(err)
		}
		for _, r := range results {
			if r.fansOut() {
				t.Errorf("allowlisted statement still flagged: %v", r)
			}
		}
	})

	t.Run("update of the key moves rows", func(t *testing.T) {
		rec := &queryRecorder{}
		pool := newPartitionModePool(t, "partition-guard-move", rec)
		migrateAndPartition(t, pool, rec)
		ctx := t.Context()
		if _, err := pool.Exec(ctx, `
			INSERT INTO atespaces (name, uid, version, proto) VALUES ('a', 'ua', 1, ''), ('b', 'ub', 1, '');
			INSERT INTO actors (atespace, name, uid, version, proto) VALUES ('a', 'x', 'ux', 1, '')`); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `UPDATE actors SET atespace = 'b' WHERE atespace = 'a' AND name = 'x'`)
		if err == nil || !strings.Contains(err.Error(), "partition mode: UPDATE of actors.atespace moves rows between partitions") {
			t.Fatalf("UPDATE error = %v, want partition key guard", err)
		}
		t.Log(err)
	})
}
