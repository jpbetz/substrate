//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package atepg

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// partitionKeys declares the column each table would be partitioned by if it
// were ever partitioned. TestPartitionReadiness fails on any schema change
// that PostgreSQL would reject when converting the table to
// PARTITION BY HASH (key), so the option stays open without paying for it.
//
// Only actors is enforced today. actor_templates, actor_snapshots,
// actor_snapshot_tags and actor_egress_policies are atespace-scoped in the
// same way and are candidates for the same entry.
var partitionKeys = map[string]string{
	"actors": "atespace",
}

func TestPartitionReadiness(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()
	var schema string
	if err := p.pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("reading current schema: %v", err)
	}
	for table, key := range partitionKeys {
		for _, violation := range partitionReadinessViolations(ctx, t, p.pool, schema, table, key) {
			t.Error(violation)
		}
	}
}

// partitionReadinessViolations returns one message per catalog object that
// would stop PostgreSQL from partitioning schema.table by key. It reads only
// pg_catalog, so it sees the schema as migrations left it, not the SQL the
// store runs against it.
func partitionReadinessViolations(ctx context.Context, t *testing.T, pool *pgxpool.Pool, schema, table, key string) []string {
	t.Helper()
	rel := pgx.Identifier{schema, table}.Sanitize()
	var violations []string
	fail := func(format string, args ...any) {
		violations = append(violations, fmt.Sprintf("%s.%s: ", schema, table)+fmt.Sprintf(format, args...))
	}

	// 1. The key column must exist and be NOT NULL: a NULL cannot be routed
	// to a partition.
	var keyAttnum int16
	var notNull bool
	err := pool.QueryRow(ctx, `
		SELECT attnum, attnotnull FROM pg_attribute
		WHERE attrelid = $1::regclass AND attname = $2 AND NOT attisdropped`, rel, key).Scan(&keyAttnum, &notNull)
	if err != nil {
		if err == pgx.ErrNoRows {
			fail("partition key column %q does not exist", key)
			return violations
		}
		t.Fatalf("reading column %s.%s: %v", table, key, err)
	}
	if !notNull {
		fail("partition key column %q must be NOT NULL: a NULL key cannot be routed to a partition", key)
	}

	// 2. Every unique index, which includes PRIMARY KEY and UNIQUE
	// constraints, must have key among its key columns. PostgreSQL enforces
	// uniqueness per partition, so an index without the partition key cannot
	// promise global uniqueness. A 0 in indkey is an expression, never key.
	// indkey is an int2vector, which is zero-based; INCLUDE columns sit past
	// indnkeyatts and do not count.
	rows, err := pool.Query(ctx, `
		SELECT c.relname, pg_get_indexdef(i.indexrelid), (i.indkey::int2[])[0:i.indnkeyatts-1]
		FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE i.indrelid = $1::regclass AND i.indisunique
		ORDER BY c.relname`, rel)
	if err != nil {
		t.Fatalf("listing unique indexes on %s: %v", table, err)
	}
	for _, idx := range mustCollect3[string, string, []int16](t, rows) {
		if !slices.Contains(idx.c, keyAttnum) {
			fail("unique index %s (%s) omits partition key %q: PostgreSQL requires every unique constraint on a partitioned table to include all partition key columns, so this index would block partitioning and can never be globally unique across partitions", idx.a, idx.b, key)
		}
	}

	// 3. EXCLUDE constraints follow the same rule as unique indexes.
	rows, err = pool.Query(ctx, `
		SELECT conname, pg_get_constraintdef(oid), conkey
		FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'x'
		ORDER BY conname`, rel)
	if err != nil {
		t.Fatalf("listing exclusion constraints on %s: %v", table, err)
	}
	for _, con := range mustCollect3[string, string, []int16](t, rows) {
		if !slices.Contains(con.c, keyAttnum) {
			fail("exclusion constraint %s (%s) omits partition key %q: PostgreSQL requires every exclusion constraint on a partitioned table to include all partition key columns", con.a, con.b, key)
		}
	}

	// 4. Every foreign key referencing this table must reference a column
	// set that includes key. Foreign keys can only target a unique key, and
	// rule 2 says every unique key includes the partition key, so a foreign
	// key on other columns (typically uid) both needs an index rule 2
	// forbids and pins the table to a lookup partitioning cannot serve.
	rows, err = pool.Query(ctx, `
		SELECT conname, conrelid::regclass::text, pg_get_constraintdef(oid), confkey
		FROM pg_constraint WHERE confrelid = $1::regclass AND contype = 'f'
		ORDER BY conname`, rel)
	if err != nil {
		t.Fatalf("listing foreign keys referencing %s: %v", table, err)
	}
	type fk struct {
		name, from, def string
		cols            []int16
	}
	var fks []fk
	for rows.Next() {
		var f fk
		if err := rows.Scan(&f.name, &f.from, &f.def, &f.cols); err != nil {
			t.Fatalf("scanning foreign key: %v", err)
		}
		fks = append(fks, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("listing foreign keys referencing %s: %v", table, err)
	}
	for _, f := range fks {
		if !slices.Contains(f.cols, keyAttnum) {
			fail("foreign key %s on %s (%s) references columns that omit partition key %q: a partitioned table can only be referenced through a unique key that includes its partition key", f.name, f.from, f.def, key)
		}
	}
	return violations
}

type triple[A, B, C any] struct {
	a A
	b B
	c C
}

func mustCollect3[A, B, C any](t *testing.T, rows pgx.Rows) []triple[A, B, C] {
	t.Helper()
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (triple[A, B, C], error) {
		var v triple[A, B, C]
		err := row.Scan(&v.a, &v.b, &v.c)
		return v, err
	})
	if err != nil {
		t.Fatalf("collecting rows: %v", err)
	}
	return out
}

// TestPartitionReadinessDetectsViolations proves each rule fires, and that
// PostgreSQL itself rejects the same schema when asked to partition it, so
// the rules track the server rather than a guess.
func TestPartitionReadinessDetectsViolations(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "partition-readiness"
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`); err != nil {
		t.Fatalf("resetting PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`)
	})
	p, err := Connect(ctx, containerDSN, schema)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	t.Cleanup(func() { p.Close(); p.pool.Close() })
	db := p.pool // search_path is the test schema.

	check := func(t *testing.T, table, key string) []string {
		t.Helper()
		return partitionReadinessViolations(ctx, t, db, schema, table, key)
	}
	exec := func(t *testing.T, sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	// postgresRejects asserts PostgreSQL fails sql with a message containing
	// want, and rolls back anything it may have done.
	postgresRejects := func(t *testing.T, sql, want string) {
		t.Helper()
		_, err := db.Exec(ctx, sql)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("PostgreSQL error for %q = %v, want one containing %q", sql, err, want)
		}
		t.Logf("PostgreSQL agrees: %v", err)
	}
	expectViolation := func(t *testing.T, violations []string, want string) {
		t.Helper()
		for _, v := range violations {
			if strings.Contains(v, want) {
				t.Logf("violation: %s", v)
				return
			}
		}
		t.Fatalf("violations %q lack one containing %q", violations, want)
	}

	if v := check(t, "actors", "atespace"); len(v) != 0 {
		t.Fatalf("clean schema reported violations: %q", v)
	}
	// The clean schema partitions as declared.
	exec(t, `CREATE TABLE actors_part (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace)`)

	t.Run("UniqueIndexWithoutKey", func(t *testing.T) {
		exec(t, `CREATE UNIQUE INDEX actors_uid_key ON actors (uid)`)
		defer exec(t, `DROP INDEX actors_uid_key`)
		expectViolation(t, check(t, "actors", "atespace"), `unique index actors_uid_key (CREATE UNIQUE INDEX actors_uid_key ON "partition-readiness".actors USING btree (uid)) omits partition key "atespace"`)
		postgresRejects(t, `CREATE TABLE actors_bad (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace)`, "unique constraint on partitioned table must include all partitioning columns")
	})

	t.Run("PartialUniqueIndexWithoutKey", func(t *testing.T) {
		exec(t, `CREATE UNIQUE INDEX actors_name_live ON actors (name) WHERE version > 0`)
		defer exec(t, `DROP INDEX actors_name_live`)
		expectViolation(t, check(t, "actors", "atespace"), `unique index actors_name_live (`)
		postgresRejects(t, `CREATE TABLE actors_bad (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace)`, "unique constraint on partitioned table must include all partitioning columns")
	})

	t.Run("ExpressionUniqueIndexWithoutKey", func(t *testing.T) {
		exec(t, `CREATE UNIQUE INDEX actors_lower_uid ON actors (lower(uid))`)
		defer exec(t, `DROP INDEX actors_lower_uid`)
		expectViolation(t, check(t, "actors", "atespace"), `unique index actors_lower_uid (`)
		postgresRejects(t, `CREATE TABLE actors_bad (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace)`, "unique constraint on partitioned table must include all partitioning columns")
	})

	t.Run("UniqueConstraintWithKeyAsIncludeOnly", func(t *testing.T) {
		// INCLUDE columns are payload, not key columns; PostgreSQL does not
		// count them toward the partition key.
		exec(t, `CREATE UNIQUE INDEX actors_uid_incl ON actors (uid) INCLUDE (atespace)`)
		defer exec(t, `DROP INDEX actors_uid_incl`)
		expectViolation(t, check(t, "actors", "atespace"), `unique index actors_uid_incl (`)
		postgresRejects(t, `CREATE TABLE actors_bad (LIKE actors INCLUDING ALL) PARTITION BY HASH (atespace)`, "unique constraint on partitioned table must include all partitioning columns")
	})

	t.Run("ForeignKeyToNonKeyColumns", func(t *testing.T) {
		exec(t, `CREATE UNIQUE INDEX actors_uid_key ON actors (uid)`)
		defer exec(t, `DROP INDEX actors_uid_key`)
		exec(t, `CREATE TABLE actor_refs (actor_uid text NOT NULL REFERENCES actors (uid))`)
		defer exec(t, `DROP TABLE actor_refs`)
		violations := check(t, "actors", "atespace")
		expectViolation(t, violations, `foreign key actor_refs_actor_uid_fkey on actor_refs (FOREIGN KEY (actor_uid) REFERENCES actors(uid)) references columns that omit partition key "atespace"`)
		// The partitioned copy has no unique key on uid, and rule 2 says it
		// never can, so the same foreign key cannot be pointed at it.
		postgresRejects(t, `ALTER TABLE actor_refs ADD FOREIGN KEY (actor_uid) REFERENCES actors_part (uid)`, `there is no unique constraint matching given keys for referenced table "actors_part"`)
	})

	t.Run("ForeignKeyIncludingKeyIsAllowed", func(t *testing.T) {
		// actor_egress_policies already references actors(atespace, name);
		// a partitioned table accepts that shape.
		exec(t, `CREATE TABLE actor_part_refs (atespace text NOT NULL, actor_name text NOT NULL, FOREIGN KEY (atespace, actor_name) REFERENCES actors_part (atespace, name))`)
		defer exec(t, `DROP TABLE actor_part_refs`)
		if v := check(t, "actors", "atespace"); len(v) != 0 {
			t.Fatalf("unexpected violations: %q", v)
		}
	})

	t.Run("NullableOrMissingKey", func(t *testing.T) {
		exec(t, `CREATE TABLE scratch (atespace text, name text NOT NULL PRIMARY KEY)`)
		defer exec(t, `DROP TABLE scratch`)
		expectViolation(t, check(t, "scratch", "atespace"), `partition key column "atespace" must be NOT NULL`)
		expectViolation(t, check(t, "scratch", "tenant"), `partition key column "tenant" does not exist`)
	})

	t.Run("ExclusionConstraintWithoutKey", func(t *testing.T) {
		exec(t, `CREATE TABLE scratch (atespace text NOT NULL, name text NOT NULL, EXCLUDE USING btree (name WITH =))`)
		defer exec(t, `DROP TABLE scratch`)
		expectViolation(t, check(t, "scratch", "atespace"), `exclusion constraint scratch_name_excl (EXCLUDE USING btree (name WITH =)) omits partition key "atespace"`)
		postgresRejects(t, `CREATE TABLE scratch_bad (LIKE scratch INCLUDING ALL) PARTITION BY HASH (atespace)`, "on partitioned table must include all partitioning columns")
	})
}
