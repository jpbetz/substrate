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

// Static partition-readiness scan.
//
// The tables below are expected to be partitionable by the named key column
// without rewriting any query. This test parses the package's non-test Go
// sources, collects every SQL string literal that addresses one of those
// tables, and checks that the statement would prune to one partition:
//
//   - INSERT names the key column.
//   - SELECT, UPDATE and DELETE carry a `key = $n` (or literal) predicate in
//     every conjunct-branch of their WHERE clause.
//   - UPDATE never assigns the key column (row movement across partitions).
//
// A statement that legitimately spans partitions, such as a global listing,
// declares that with a `-- cross-partition: <reason>` comment inside the SQL
// text, or by adding its fingerprint to crossPartitionAllowlist.
//
// This is a heuristic guard over source text, not a SQL parser. It sees every
// statement written in the package, including ones no test executes, but it
// cannot follow SQL assembled at runtime and it does not understand planner
// equivalence classes. Keep the statements it inspects simple.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// partitionKeys maps each table that must stay partition-ready to its
// intended partition key. Candidates not yet enforced: actor_templates,
// actor_snapshots, actor_snapshot_tags, actor_egress_policies (all keyed by
// atespace).
var partitionKeys = map[string]string{
	"actors": "atespace",
}

// crossPartitionAllowlist holds fingerprints (see sqlFingerprint) of
// statements that are allowed to touch every partition. Prefer the in-SQL
// `-- cross-partition:` annotation; this list exists for SQL that cannot
// carry a comment.
var crossPartitionAllowlist = map[string]string{}

const crossPartitionMarker = "-- cross-partition:"

type sqlFinding struct {
	Pos  string // file:line of the literal
	SQL  string // normalized statement
	Rule string
}

func (f sqlFinding) String() string {
	return fmt.Sprintf("%s: %s\n    %s", f.Pos, f.Rule, f.SQL)
}

func TestPartitionSQLScan(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	// ATEPG_PARTITION_SCAN_FILES points the scan at other Go files instead,
	// for checking a branch's sources without building them.
	if extra := os.Getenv("ATEPG_PARTITION_SCAN_FILES"); extra != "" {
		entries = filepath.SplitList(extra)
	}
	var findings []sqlFinding
	scanned := 0
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fs, err := scanGoSourceForSQL(filepath.Base(path), string(src))
		if err != nil {
			t.Fatal(err)
		}
		findings = append(findings, fs...)
		scanned++
	}
	if scanned == 0 {
		t.Fatal("no Go sources scanned")
	}
	for _, f := range findings {
		t.Errorf("%s", f)
	}
}

// scanGoSourceForSQL parses one Go file and returns the partition findings
// for every SQL literal in it.
func scanGoSourceForSQL(filename, src string) ([]sqlFinding, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	var findings []sqlFinding
	// Concatenations are folded at the BinaryExpr level; the inner literals
	// are then skipped so they are not reported twice.
	seen := map[ast.Node]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		if seen[n] {
			return false
		}
		switch x := n.(type) {
		case *ast.BinaryExpr:
			if x.Op != token.ADD {
				return true
			}
			text, dynamic, ok := foldStringConcat(x, seen)
			if !ok {
				return true
			}
			findings = append(findings, checkSQLText(fset.Position(x.Pos()).String(), text, dynamic)...)
			return false
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(x.Value)
			if err != nil {
				return true
			}
			findings = append(findings, checkSQLText(fset.Position(x.Pos()).String(), text, false)...)
		}
		return true
	})
	return findings, nil
}

// foldStringConcat flattens a chain of `+` whose operands are string
// literals or arbitrary expressions. Non-literal operands become a
// placeholder and set dynamic. ok is false when no operand is a string
// literal (an arithmetic expression, for instance).
func foldStringConcat(e ast.Expr, seen map[ast.Node]bool) (text string, dynamic, ok bool) {
	switch x := e.(type) {
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", true, false
		}
		l, ld, lok := foldStringConcat(x.X, seen)
		r, rd, rok := foldStringConcat(x.Y, seen)
		if !lok && !rok {
			return "", true, false
		}
		if !lok {
			l, ld = " <expr> ", true
		}
		if !rok {
			r, rd = " <expr> ", true
		}
		seen[x.X], seen[x.Y] = true, true
		return l + r, ld || rd, true
	case *ast.ParenExpr:
		return foldStringConcat(x.X, seen)
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", true, false
		}
		s, err := strconv.Unquote(x.Value)
		if err != nil {
			return "", true, false
		}
		return s, false, true
	default:
		return " <expr> ", true, false
	}
}

var (
	sqlCommentRE  = regexp.MustCompile(`--[^\n]*`)
	whitespaceRE  = regexp.MustCompile(`\s+`)
	sqlLeadRE     = regexp.MustCompile(`(?i)^\s*(select|insert|update|delete|with)\b`)
	clauseStopRE  = regexp.MustCompile(`(?i)\b(order by|group by|limit|returning|for update|for share|having|window|union|except|intersect)\b`)
	insertColsRE  = regexp.MustCompile(`(?i)^insert\s+into\s+(\w+)\s*(?:\(([^)]*)\))?`)
	updateStmtRE  = regexp.MustCompile(`(?i)^update\s+(\w+)(?:\s+as)?(?:\s+(\w+))?\s+set\s+(.*?)(?:\s+where\s+(.*))?$`)
	whereSplitRE  = regexp.MustCompile(`(?i)\bwhere\b`)
	andSplitRE    = regexp.MustCompile(`(?i)\s+and\s+`)
	orSplitRE     = regexp.MustCompile(`(?i)\s+or\s+`)
	setAssignRE   = regexp.MustCompile(`(?i)(^|,)\s*(\w+\.)?(\w+)\s*=`)
	stmtSplitRE   = regexp.MustCompile(`;`)
	tableRefCache = map[string]*regexp.Regexp{}
)

// tableRef reports whether stmt addresses table and, if so, the alias it
// uses (the table name itself when there is none).
func tableRef(stmt, table string) (alias string, ok bool) {
	re := tableRefCache[table]
	if re == nil {
		// Go's regexp has no lookahead, so the alias is captured naively and
		// keywords are filtered afterwards.
		re = regexp.MustCompile(fmt.Sprintf(`(?i)\b(from|join|into|update)\s+%s\b(?:\s+as)?(?:\s+(\w+))?`, regexp.QuoteMeta(table)))
		tableRefCache[table] = re
	}
	m := re.FindStringSubmatch(stmt)
	if m == nil {
		return "", false
	}
	alias = m[2]
	switch strings.ToLower(alias) {
	case "", "where", "set", "on", "using", "values", "select", "returning", "for", "order", "group", "limit",
		"left", "right", "inner", "outer", "cross", "join", "natural", "as":
		alias = table
	}
	return alias, true
}

// normalizeSQL strips comments and collapses whitespace.
func normalizeSQL(s string) string {
	s = sqlCommentRE.ReplaceAllString(s, " ")
	return strings.TrimSpace(whitespaceRE.ReplaceAllString(s, " "))
}

// sqlFingerprint is the allowlist key: the normalized statement, lowercased.
func sqlFingerprint(stmt string) string {
	return strings.ToLower(normalizeSQL(stmt))
}

// checkSQLText inspects one string literal. Literals that do not start with
// a SQL verb are ignored (log messages, identifiers), as are statements that
// do not address a guarded table.
func checkSQLText(pos, text string, dynamic bool) []sqlFinding {
	if !sqlLeadRE.MatchString(text) {
		return nil
	}
	var findings []sqlFinding
	for _, raw := range stmtSplitRE.Split(text, -1) {
		if !sqlLeadRE.MatchString(raw) {
			continue
		}
		crossPartition := strings.Contains(raw, crossPartitionMarker)
		stmt := normalizeSQL(raw)
		for table, key := range partitionKeys {
			alias, ok := tableRef(stmt, table)
			if !ok {
				continue
			}
			if crossPartition || crossPartitionAllowlist[sqlFingerprint(raw)] != "" {
				continue
			}
			if dynamic {
				findings = append(findings, sqlFinding{pos, stmt, fmt.Sprintf("SQL against %s is assembled at runtime; the scan cannot verify it prunes on %s", table, key)})
				continue
			}
			if rule := checkStatement(stmt, table, alias, key); rule != "" {
				findings = append(findings, sqlFinding{pos, stmt, rule})
			}
		}
	}
	return findings
}

// checkStatement returns the broken rule, or "" when stmt prunes on key.
func checkStatement(stmt, table, alias, key string) string {
	lower := strings.ToLower(stmt)
	switch {
	case strings.HasPrefix(lower, "insert"):
		m := insertColsRE.FindStringSubmatch(stmt)
		if m == nil || m[2] == "" {
			return fmt.Sprintf("INSERT into %s must list its columns, including %s", table, key)
		}
		for _, c := range strings.Split(m[2], ",") {
			if strings.EqualFold(strings.TrimSpace(c), key) {
				return ""
			}
		}
		return fmt.Sprintf("INSERT into %s does not set %s", table, key)
	case strings.HasPrefix(lower, "update"):
		m := updateStmtRE.FindStringSubmatch(stmt)
		if m == nil {
			return fmt.Sprintf("UPDATE of %s has a shape the scan does not understand", table)
		}
		set := clauseStopRE.Split(m[3], 2)[0]
		for _, a := range setAssignRE.FindAllStringSubmatch(set, -1) {
			if strings.EqualFold(a[3], key) {
				return fmt.Sprintf("UPDATE of %s assigns partition key %s (row movement)", table, key)
			}
		}
		if m[4] == "" {
			return fmt.Sprintf("UPDATE of %s has no WHERE clause", table)
		}
		return checkWhere(m[4], table, alias, key)
	case strings.HasPrefix(lower, "with"):
		return fmt.Sprintf("CTE statement touching %s is not analyzed; simplify it or mark it %s <reason>", table, crossPartitionMarker)
	default: // SELECT, DELETE
		parts := whereSplitRE.Split(stmt, 2)
		if len(parts) < 2 {
			return fmt.Sprintf("statement on %s has no WHERE clause, so it scans every partition; mark it %s <reason> if intended", table, crossPartitionMarker)
		}
		return checkWhere(parts[1], table, alias, key)
	}
}

// checkWhere requires a key-equality predicate that holds on every path
// through the WHERE clause: some top-level AND conjunct must be the predicate
// itself, or a parenthesized OR whose every branch satisfies the same rule.
func checkWhere(where, table, alias, key string) string {
	where = clauseStopRE.Split(where, 2)[0]
	if !predicatePrunes(where, alias, key) {
		return fmt.Sprintf("WHERE clause on %s has no %s = $n predicate on every branch, so it cannot prune to one partition", table, key)
	}
	return ""
}

func predicatePrunes(expr string, alias, key string) bool {
	expr = strings.TrimSpace(expr)
	for _, conj := range splitTopLevel(expr, andSplitRE) {
		if branchPrunes(conj, alias, key) {
			return true
		}
	}
	return false
}

func branchPrunes(expr string, alias, key string) bool {
	expr = trimOuterParens(strings.TrimSpace(expr))
	if keyEquality(alias, key).MatchString(expr) {
		return true
	}
	parts := splitTopLevel(expr, orSplitRE)
	if len(parts) > 1 {
		for _, p := range parts {
			if !predicatePrunes(p, alias, key) {
				return false
			}
		}
		return true
	}
	if conj := splitTopLevel(expr, andSplitRE); len(conj) > 1 {
		return predicatePrunes(expr, alias, key)
	}
	return false
}

var keyEqualityCache = map[string]*regexp.Regexp{}

// keyEquality matches `key = $n`, `key = 'lit'`, or a row-value equality
// whose first column is key, optionally qualified by the table alias.
func keyEquality(alias, key string) *regexp.Regexp {
	k := alias + "\x00" + key
	if re := keyEqualityCache[k]; re != nil {
		return re
	}
	q := fmt.Sprintf(`(?:%s\.)?`, regexp.QuoteMeta(alias))
	re := regexp.MustCompile(fmt.Sprintf(`(?i)^(?:%s%s\s*=\s*(?:\$\d+|'[^']*')(?:::\w+)?|\(\s*%s%s\s*,[^)]*\)\s*=\s*\([^)]*\))$`, q, key, q, key))
	keyEqualityCache[k] = re
	return re
}

// splitTopLevel splits expr on sep occurrences that are not inside
// parentheses.
func splitTopLevel(expr string, sep *regexp.Regexp) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth != 0 {
			continue
		}
		if loc := sep.FindStringIndex(expr[i:]); loc != nil && loc[0] == 0 {
			parts = append(parts, expr[start:i])
			i += loc[1] - 1
			start = i + 1
		}
	}
	return append(parts, expr[start:])
}

func trimOuterParens(s string) string {
	for strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		depth := 0
		for i := 0; i < len(s); i++ {
			switch s[i] {
			case '(':
				depth++
			case ')':
				depth--
			}
			if depth == 0 && i < len(s)-1 {
				return s
			}
		}
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}
