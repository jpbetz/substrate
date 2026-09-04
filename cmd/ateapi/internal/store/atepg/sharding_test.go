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
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAtespaceTablesShardable exists to keep it possible to move each
// atespace's tables to a database of their own later. It fails on a foreign
// key between those tables and the global ones, and on a new table that is
// not classified as one or the other.
func TestAtespaceTablesShardable(t *testing.T) {
	// exemptions are foreign keys allowed to cross between an atespace's
	// tables and the global ones. Only add exceptions after
	// agreement in the community.
	var exemptions []string

	pool := migratedPool(t, "shardable")
	violations, err := shardingViolations(t.Context(), pool, exemptions)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range violations {
		t.Error(v)
	}

	t.Run("rejects a foreign key to a global table", func(t *testing.T) {
		pool := migratedPool(t, "shardable-fk")
		if _, err := pool.Exec(t.Context(), `ALTER TABLE actors ADD COLUMN worker text REFERENCES workers (name)`); err != nil {
			t.Fatal(err)
		}
		violations, err := shardingViolations(t.Context(), pool, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(violations) != 1 || !strings.Contains(violations[0], "actors_worker_fkey") {
			t.Fatalf("violations = %q, want exactly the actors to workers foreign key", violations)
		}
		t.Log(violations[0])
	})
	t.Run("rejects an unclassified table", func(t *testing.T) {
		pool := migratedPool(t, "shardable-table")
		if _, err := pool.Exec(t.Context(), `CREATE TABLE notes (id int PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		violations, err := shardingViolations(t.Context(), pool, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(violations) != 1 || !strings.Contains(violations[0], "table notes is neither") {
			t.Fatalf("violations = %q, want exactly the unclassified notes table", violations)
		}
		t.Log(violations[0])
	})
}

var (
	// atespaceTables hold one atespace's state and move with it.
	atespaceTables = append([]string{"atespaces"}, atespaceScopedTables...)
	// globalTables hold state shared by every atespace.
	globalTables = []string{"workers", "worker_outbox", "worker_outbox_trim", "leases", migrationTableName}
)

// shardingViolations reports every table that is neither an atespace table
// nor a global one, and every foreign key between the two sets other than
// the exempt ones.
func shardingViolations(ctx context.Context, pool *pgxpool.Pool, exempt []string) ([]string, error) {
	var violations []string
	rows, err := pool.Query(ctx, `
		SELECT c.relname FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p') AND NOT c.relispartition
		ORDER BY c.relname`)
	if err != nil {
		return nil, err
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	for _, table := range tables {
		if !slices.Contains(atespaceTables, table) && !slices.Contains(globalTables, table) {
			violations = append(violations, fmt.Sprintf("table %s is neither an atespace table nor a global table; add it to one in TestAtespaceTablesShardable", table))
		}
	}

	rows, err = pool.Query(ctx, `
		SELECT conname, conrelid::regclass::text, confrelid::regclass::text FROM pg_constraint
		WHERE contype = 'f' AND conparentid = 0
		ORDER BY conname`)
	if err != nil {
		return nil, err
	}
	fks, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct{ Name, From, To string }])
	if err != nil {
		return nil, err
	}
	for _, fk := range fks {
		if slices.Contains(exempt, fk.Name) {
			continue
		}
		if slices.Contains(atespaceTables, fk.From) != slices.Contains(atespaceTables, fk.To) {
			violations = append(violations, fmt.Sprintf("foreign key %s from %s to %s crosses between an atespace's tables and the global ones, so the atespace could not move to its own database; remove it or add it to the exemptions in TestAtespaceTablesShardable", fk.Name, fk.From, fk.To))
		}
	}
	return violations, nil
}
