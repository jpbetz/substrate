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
	"strings"
	"testing"
)

// TestPartitionSQLScanCases feeds synthetic Go sources through the scanner
// to pin down what it flags and what it lets through.
func TestPartitionSQLScanCases(t *testing.T) {
	cases := []struct {
		name string
		src  string // Go expression placed in a var declaration
		want string // substring of the expected rule; "" means no finding
	}{
		// Statements that must be flagged.
		{"uid lookup", "`SELECT proto FROM actors WHERE uid = $1`", "no atespace = $n predicate"},
		{"update moves key", "`UPDATE actors SET atespace = $1, version = $2 WHERE atespace = $3 AND name = $4`", "assigns partition key atespace (row movement)"},
		{"delete by name", "`DELETE FROM actors WHERE name = $1`", "no atespace = $n predicate"},
		{"insert omits key", "`INSERT INTO actors (name, uid, version, proto) VALUES ($1, $2, $3, $4)`", "does not set atespace"},
		{"insert select no columns", "`INSERT INTO actors SELECT * FROM actors_old`", "must list its columns"},
		{"join filtered via other table", "`SELECT a.proto FROM worker_assignments w JOIN actors a ON a.uid = w.actor_uid WHERE w.worker_name = $1`", "no atespace = $n predicate"},
		{"or branch without key", "`SELECT proto FROM actors WHERE atespace = $1 OR name = $2`", "no atespace = $n predicate"},
		{"no where", "`SELECT count(*) FROM actors`", "no WHERE clause"},
		{"group by all actors (PR 1204)", "`SELECT state, count(*) FROM actors GROUP BY state`", "no WHERE clause"},
		{"update without where", "`UPDATE actors SET version = 0`", "no WHERE clause"},
		{"not equal", "`SELECT proto FROM actors WHERE atespace <> $1`", "no atespace = $n predicate"},
		{"cte", "`WITH x AS (SELECT 1) SELECT proto FROM actors WHERE atespace = $1`", "CTE statement"},
		{"dynamic concat", "`SELECT proto FROM actors WHERE ` + cond", "assembled at runtime"},
		{"reversed equality (false positive)", "`SELECT proto FROM actors WHERE $1 = atespace AND name = $2`", "no atespace = $n predicate"},
		{"IN list (false positive; would prune)", "`SELECT proto FROM actors WHERE atespace IN ($1, $2)`", "no atespace = $n predicate"},
		{"join equivalence class (false positive; would prune)", "`SELECT a.proto FROM workers w JOIN actors a ON a.atespace = w.atespace WHERE w.atespace = $1`", "no atespace = $n predicate"},

		// Statements that must pass.
		{"point get", "`SELECT proto FROM actors WHERE atespace = $1 AND name = $2`", ""},
		{"key last", "`SELECT proto FROM actors WHERE name = $2 AND atespace = $1`", ""},
		{"state get (PR 1204)", "`SELECT state FROM actors WHERE atespace = $1 AND name = $2`", ""},
		{"insert with state (PR 1204)", "`INSERT INTO actors (atespace, name, uid, version, state, proto) VALUES ($1, $2, $3, $4, $5, $6)`", ""},
		{"cas update", "`UPDATE actors SET version = $1, proto = $2 WHERE atespace = $3 AND name = $4 AND uid = $5 AND version = $6`", ""},
		{"delete", "`DELETE FROM actors WHERE atespace = $1 AND name = $2`", ""},
		{"for update", "`SELECT proto FROM actors WHERE atespace = $1 AND name = $2 FOR UPDATE`", ""},
		{"scoped list", "`SELECT name, proto FROM actors WHERE atespace = $1 AND ($2::text IS NULL OR name > $2) ORDER BY name LIMIT $3`", ""},
		{"alias", "`SELECT a.proto FROM actors AS a WHERE a.atespace = $1 AND a.name = $2`", ""},
		{"alias no as", "`DELETE FROM actors a WHERE a.atespace = $1 RETURNING a.proto`", ""},
		{"or with key on both branches", "`SELECT proto FROM actors WHERE (atespace = $1 AND name = $2) OR (atespace = $1 AND name = $3)`", ""},
		{"nested or under key", "`SELECT proto FROM actors WHERE atespace = $1 AND (name = $2 OR uid = $3)`", ""},
		{"row value equality", "`SELECT proto FROM actors WHERE (atespace, name) = ($1, $2)`", ""},
		{"literal key", "`SELECT proto FROM actors WHERE atespace = 'system'`", ""},
		{"typed param", "`SELECT proto FROM actors WHERE atespace = $1::text`", ""},
		{"annotated global list", "`-- cross-partition: global listing\n\t\tSELECT atespace, name, proto FROM actors ORDER BY atespace, name LIMIT $1`", ""},
		{"multi-statement literal", "`SELECT 1; DELETE FROM actors WHERE atespace = $1 AND name = $2`", ""},
		{"static concat", "`SELECT proto FROM actors ` + `WHERE atespace = $1 AND name = $2`", ""},

		// Text that must be ignored outright.
		{"information_schema literal", "`SELECT 1 FROM information_schema.columns WHERE table_name = 'actors'`", ""},
		{"log message", "`listing actors in %q: %w`", ""},
		{"other table with prefix", "`SELECT proto FROM actors_old WHERE uid = $1`", ""},
		{"actor_snapshots is a different table", "`SELECT proto FROM actor_snapshots WHERE uid = $1`", ""},
		{"egress policies out of scope", "`SELECT proto FROM actor_egress_policies WHERE uid = $1`", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "package p\nvar cond string\nvar q = " + tc.src + "\n"
			findings, err := scanGoSourceForSQL("case.go", src)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if tc.want == "" {
				if len(findings) != 0 {
					t.Fatalf("unexpected findings:\n%v", findings)
				}
				return
			}
			if len(findings) != 1 {
				t.Fatalf("got %d findings, want 1: %v", len(findings), findings)
			}
			if !strings.Contains(findings[0].Rule, tc.want) {
				t.Fatalf("rule = %q, want substring %q", findings[0].Rule, tc.want)
			}
			t.Logf("%s", findings[0])
		})
	}
}

// TestPartitionSQLScanAllowlist shows the fingerprint route for SQL that
// cannot carry a comment.
func TestPartitionSQLScanAllowlist(t *testing.T) {
	const sql = "SELECT state, count(*) FROM actors GROUP BY state"
	src := "package p\nvar q = `" + sql + "`\n"
	findings, err := scanGoSourceForSQL("case.go", src)
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings = %v, err = %v; want exactly one", findings, err)
	}
	key := sqlFingerprint("  select state,   count(*) FROM actors\n GROUP BY state ")
	crossPartitionAllowlist[key] = "per-state gauge"
	defer delete(crossPartitionAllowlist, key)
	findings, err = scanGoSourceForSQL("case.go", src)
	if err != nil || len(findings) != 0 {
		t.Fatalf("allowlisted findings = %v, err = %v; want none", findings, err)
	}
}
