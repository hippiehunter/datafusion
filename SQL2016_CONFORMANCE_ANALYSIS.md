# SQL:2016 Conformance Test Failure Analysis

**Current Status: 2527 passed / 139 failed (94.8% pass rate)**

---

## Executive Summary

The 139 remaining test failures have been analyzed and categorized. **42 tests can be fixed with 2-3 days of focused effort**, bringing the pass rate to 96.4%. An additional 25 tests require medium effort (1-2 weeks) to achieve 97.0% conformance.

---

## Quick Win Opportunities (42 tests, 2-3 days)

### 1. MATCH_RECOGNIZE Navigation Functions (15 tests) 🎯
**Effort: LOW (4 hours) | Impact: HIGH**

**Problem:**
- Functions FIRST, LAST, PREV, NEXT not registered
- Error: `Invalid function 'first'`

**Solution:**
Register stub scalar functions in the conformance test context provider. These functions are referenced in MATCH_RECOGNIZE MEASURES/DEFINE clauses but don't need full execution support yet - just function signatures.

**Affected Tests:**
- r010_first_function
- r010_last_function
- r010_define_prev / r010_define_prev_offset
- r010_define_next
- r010_measures_navigation
- r010_multiple_navigation
- r010_sequential_events
- r010_user_session
- r010_stock_v_pattern
- r010_multi_stage_pattern
- r010_multiple_order
- r010_skip_to_variable
- r010_match_with_join
- r010_complete_match_recognize

**Example Error:**
```sql
SELECT * FROM t MATCH_RECOGNIZE (
  ORDER BY id
  MEASURES FIRST(A.value) AS first_val
  PATTERN (A+)
  DEFINE A AS value > 0
)
```
Error: `Invalid function 'first'`

---

### 2. GRANT/REVOKE Role Support (15 tests) 🎯
**Effort: MEDIUM (6 hours) | Impact: HIGH**

**Problem:**
- Role-based privilege management not planned
- Error: `NotImplemented("Unsupported SQL statement: GRANT reporting_role TO alice")`

**Current State:**
- ✅ Parser already supports GRANT/REVOKE role syntax (via sqlparser)
- ✅ Table privilege tests work (82 of 99 privilege tests pass)
- ❌ Only role management (T331/T332) fails

**Solution:**
Modify `/home/jeff/repos/datafusion/datafusion/sql/src/statement.rs` to distinguish between:
- `GRANT SELECT ON table TO user` (works)
- `GRANT role_name TO user` (needs implementation)

Add planning support for role-to-grantee GRANT/REVOKE. Can be stub/no-op for now, similar to table privileges.

**Affected Tests:**
- t332_grant_role_to_user
- t332_grant_role_to_role
- t332_grant_multiple_roles
- t332_grant_role_with_admin_option
- t332_grant_role_to_multiple_users
- t332_revoke_role_from_user
- t332_revoke_role_from_multiple_users
- t332_revoke_multiple_roles
- t332_revoke_admin_option_for
- t332_revoke_role_cascade
- t332_revoke_role_restrict
- t331_drop_role_cascade
- t331_drop_role_restrict
- e081_summary_roles
- e081_summary_all_features

**Additional Issues:**
- e081_09_grant_usage_charset: `GRANT USAGE ON CHARACTER SET` - parser issue
- e081_10_grant_execute_specific_routine: `GRANT EXECUTE ON SPECIFIC FUNCTION` - parser issue

**Example Error:**
```sql
GRANT reporting_role TO alice
```
Error: `NotImplemented("Unsupported SQL statement: GRANT reporting_role TO alice")`

---

### 3. Row Type Dot Notation (9 tests) 🎯
**Effort: MEDIUM (8 hours) | Impact: MEDIUM**

**Problem:**
- `struct_col.field` syntax fails in planning
- Error: `Plan("could not parse compound identifier from [\"struct_col\", \"x\"]")`

**Current State:**
- ✅ Bracket syntax works: `struct_col['field']`
- ❌ SQL standard dot notation fails

**Solution:**
In the planner, when resolving identifiers:
1. Check if the first part of a compound identifier is a struct column
2. If yes, convert `[col, field]` to a field access expression
3. Handle nested access: `struct_col.nested.field`

**Affected Tests:**
- t051_row_field_access_dot
- t051_field_access_in_where
- t051_field_access_in_order_by
- t051_field_access_in_expression
- t051_multiple_field_access
- t051_nested_field_access
- t052_max_struct_with_where
- mixed_overlay_on_struct_field
- (Note: t051_create_table_nested_row has a different issue)

**Example Error:**
```sql
SELECT struct_col.x FROM struct_types
```
Error: `Plan("could not parse compound identifier from [\"struct_col\", \"x\"]")`

---

### 4. ABORT Transaction Statement (3 tests) 🎯
**Effort: LOW (2 hours) | Impact: LOW**

**Problem:**
- ABORT [WORK|TRANSACTION] not parsed (PostgreSQL extension)
- Should be an alias for ROLLBACK

**Solution:**
Add ABORT keyword support in parser/planner as an alias to ROLLBACK.

**Affected Tests:**
- abort_statement
- abort_work
- abort_transaction

**Example Error:**
```sql
ABORT
```
Error: `SQL(ParserError("Expected: an SQL statement, found: ABORT at Line: 1, Column: 1"))`

---

## Medium Effort Opportunities (25 tests, 1-2 weeks)

### Array Subquery Constructor (8 tests)
**Effort: MEDIUM | Impact: MEDIUM**

**Problem:**
- `ARRAY(SELECT ...)` not recognized as array constructor with subquery
- Error: `'array' does not support zero arguments`

Parser interprets this as `ARRAY()` function call with no arguments instead of `ARRAY(subquery)`.

**Affected Tests:**
- s095_array_constructor_subquery
- s095_array_constructor_subquery_order
- s095_array_constructor_scalar_subquery
- s095_array_constructor_join_subquery
- s095_nested_array_constructor_subquery
- s091_01_array_constructor_nulls
- edge_array_mixed_nulls
- mixed_array_lateral_join

**Example Error:**
```sql
SELECT ARRAY(SELECT a FROM t WHERE a > 10)
```
Error: `'array' does not support zero arguments`

---

### T053 All-Fields Alias (7 tests)
**Effort: HIGH (requires sqlparser update) | Impact: LOW**

**Problem:**
- `SELECT t.* AS (col1, col2, col3)` syntax not parsed
- Requires updating sqlparser dependency

**Affected Tests:**
- t053_all_fields_alias_basic
- t053_all_fields_alias_with_where
- t053_all_fields_alias_order_by
- t053_all_fields_alias_join
- t053_all_fields_alias_subquery
- t053_all_fields_alias_specific_table
- t053_multiple_all_fields_aliases

---

### Interval Arithmetic (4 tests)
**Effort: MEDIUM | Impact: LOW**

**Problem:**
- Type coercion for INTERVAL * number, INTERVAL / number operations
- Error: `Cannot coerce arithmetic expression Interval(MonthDayNano) * Int64`

**Affected Tests:**
- f052_interval_multiplication
- f052_interval_division
- f052_time_plus_interval
- f052_time_minus_interval

**Example Error:**
```sql
SELECT INTERVAL '1' DAY * 7
```
Error: `Cannot coerce arithmetic expression Interval(MonthDayNano) * Int64`

---

### CTE in DML Statements (3 tests)
**Effort: MEDIUM | Impact: MEDIUM**

**Problem:**
- WITH clause before INSERT/UPDATE/DELETE not supported
- Error: `NotImplemented("Query INSERT INTO t2 SELECT * FROM cte not implemented yet")`

**Affected Tests:**
- t121_cte_in_insert
- t121_cte_in_update
- t121_cte_in_delete

---

### UPDATE Tuple Assignment (3 tests)
**Effort: MEDIUM | Impact: LOW**

**Problem:**
- `SET (a, b) = (1, 2)` syntax not parsed

**Affected Tests:**
- t641_update_tuple_assignment
- t641_update_tuple_where
- t641_update_tuple_subquery

---

## Complex Features - Long-term (72 tests)

### PSM Routines (26 tests) - COMPLEX
**Effort: HIGH (weeks) | Impact: HIGH**

**Problem:**
Parser requires semicolons in procedure bodies. Need comprehensive PSM statement support including:
- BEGIN/END block parsing
- Control flow statements (FOR, WHILE, IF)
- Exception handlers (SIGNAL, RESIGNAL)
- Variable declarations and assignments

**Example Error:**
```sql
CREATE PROCEDURE reset_salaries()
  UPDATE person SET salary = 50000
```
Error: `Expected: ;, found: EOF`

This is the single largest category of failures but requires significant parser work.

---

### JSON_TABLE (11 tests) - COMPLEX
**Effort: VERY HIGH | Impact: LOW**

**Problem:**
Full JSON_TABLE implementation required. Complex table function with:
- JSON path expressions
- Multiple column definitions
- Error handling clauses
- Nested paths

**Example Error:**
```sql
SELECT jt.* FROM json_data,
  JSON_TABLE(data, '$' COLUMNS(name VARCHAR(100) PATH '$.name')) AS jt
```
Error: `NotImplemented("JSON_TABLE is not yet implemented")`

---

### COPY Statement (11 tests) - COMPLEX
**Effort: HIGH | Impact: LOW**

**Problem:**
B021 COPY FROM/TO syntax not in parser. PostgreSQL/SQL standard extension.

**Affected Tests:**
- b021_copy_from_basic
- b021_copy_from_csv_delimiter
- b021_copy_from_csv_header
- b021_copy_from_format_csv
- b021_copy_from_parquet
- b021_copy_from_with_columns
- b021_copy_to_with_columns
- b021_prepare_parameters_question
- b021_use_database_explicit
- b021_use_schema
- b021_vacuum_analyze

---

### Miscellaneous (24 tests) - VARIES

Various edge cases across different features:
- CREATE TABLE AS with column list (1 test)
- VIEW WITH CHECK OPTION (3 tests)
- Recursive CTE features (2 tests)
- SUBSTRING with SIMILAR (4 tests)
- Window NTH_VALUE IGNORE NULLS (1 test)
- Other JSON features (4 tests)
- Correlated DELETE (1 test)
- FETCH FIRST PERCENT (1 test)
- Complex ORDER BY scenarios (2 tests)
- UNNEST WITH ORDINALITY (1 test)
- Array bounds/lateral joins (3 tests)
- INSERT DEFAULT VALUES (1 test)

---

## Recommended Implementation Plan

### Week 1: Quick Wins (33 tests)
1. **MATCH_RECOGNIZE navigation functions** (15 tests, ~4 hours)
   - Highest value, lowest effort
   - Just register stub functions with appropriate signatures

2. **GRANT/REVOKE role support** (15 tests, ~6 hours)
   - High value, medium effort
   - Parser already works, need planner logic

3. **ABORT statement** (3 tests, ~2 hours)
   - Low hanging fruit
   - Simple alias to ROLLBACK

**Result: 2560/2666 tests passing (96.1%)**

---

### Week 2: Medium Quick Wins (17 tests)
4. **Struct field dot notation** (9 tests, ~8 hours)
   - Important SQL standard compliance feature
   - Moderate complexity in planner

5. **Array subquery constructor** (8 tests, ~8 hours)
   - Parser/planner coordination
   - Useful SQL feature

**Result: 2577/2666 tests passing (96.7%)**

---

### Week 3-4: Additional Medium Effort (10+ tests)
6. **Interval arithmetic** (4 tests)
   - Type coercion logic

7. **CTE in DML** (3 tests)
   - Parser support for WITH before INSERT/UPDATE/DELETE

8. **UPDATE tuple assignment** (3 tests)
   - Parser support for SET (a,b) = (1,2)

9. **Miscellaneous quick fixes** (~5-10 tests)
   - INSERT DEFAULT VALUES
   - Small parser/planner additions

**Result: 2590+/2666 tests passing (97.2%+)**

---

### Long-term Backlog
- PSM routines (26 tests) - requires weeks of parser work
- JSON_TABLE (11 tests) - complex feature implementation
- COPY statement (11 tests) - parser extension
- Remaining edge cases (24 tests) - various features

---

## Key Insights

1. **Low-hanging fruit exists**: 42 tests can be fixed in 2-3 days
2. **Privileges mostly work**: 82/99 tests pass, only role management remains
3. **PSM is the biggest blocker**: 26 tests, requires significant parser work
4. **Most failures are edge cases**: Complex or low-priority SQL features
5. **Parser readiness varies**: Some features parse but don't plan, others need parser updates

---

## Files to Modify (for quick wins)

1. `/home/jeff/repos/datafusion/datafusion/sql/tests/conformance.rs`
   - Add MATCH_RECOGNIZE navigation function stubs to context provider

2. `/home/jeff/repos/datafusion/datafusion/sql/src/statement.rs`
   - Add GRANT/REVOKE role planning logic
   - Add ABORT statement handling

3. `/home/jeff/repos/datafusion/datafusion/sql/src/planner.rs` or `relation/mod.rs`
   - Add compound identifier handling for struct field access
