# SQL:2016 Conformance Test Suite

This directory contains tests that track DataFusion's conformance to the
ISO/IEC 9075:2016 SQL standard (SQL:2016).

## Overview

The SQL standard defines approximately **179 Core features** (mandatory) and
**~400 optional features** across 9 parts. This test suite aims to provide
a test for each feature, making it easy to track conformance progress.

## Directory Structure

```
conformance/
├── mod.rs                    # Test harness, macros, utilities
├── README.md                 # This file
├── part2_foundation/         # SQL/Foundation (Part 2) - Core SQL
│   ├── mod.rs
│   ├── e011_numeric_types.rs
│   ├── e021_character_types.rs
│   └── ...
├── part3_cli/                # SQL/CLI (Part 3)
├── part4_psm/                # SQL/PSM (Part 4)
├── part9_med/                # SQL/MED (Part 9)
├── part11_schemata/          # SQL/Schemata (Part 11)
└── part14_xml/               # SQL/XML (Part 14)
```

## Running Tests

```bash
# Run all conformance tests
cargo test -p datafusion-sql --test conformance

# Run specific feature
cargo test -p datafusion-sql --test conformance e011

# Run specific part
cargo test -p datafusion-sql --test conformance part2_foundation

# Show test output
cargo test -p datafusion-sql --test conformance -- --nocapture
```

## Test Macros

| Macro | Purpose |
|-------|---------|
| `assert_parses!` | Verify SQL text parses without error |
| `assert_plans!` | Verify SQL converts to logical plan |
| `assert_feature_supported!` | Verify feature works (parse + plan) |
| `assert_not_implemented!` | Mark feature as not yet implemented |
| `assert_parse_error!` | Verify SQL fails to parse |
| `assert_plan_error!` | Verify SQL fails to plan |

## Conformance Status

### Part 2: SQL/Foundation (Core SQL)

| Feature ID | Description | Status | Tests |
|------------|-------------|--------|-------|
| E011 | Numeric data types | Supported | e011_numeric_types.rs |
| E011-01 | INTEGER and SMALLINT | Supported | |
| E011-02 | REAL, DOUBLE, FLOAT | Supported | |
| E011-03 | DECIMAL and NUMERIC | Supported | |
| E011-04 | Arithmetic operators | Supported | |
| E011-05 | Numeric comparison | Supported | |
| E011-06 | Implicit casting | Supported | |
| E021 | Character data types | TODO | |
| E031 | Identifiers | TODO | |
| E051 | Basic query specification | TODO | |
| E061 | Basic predicates | TODO | |
| E071 | Basic query expressions | TODO | |
| E081 | Basic privileges | TODO | |
| E091 | Set functions | TODO | |
| E101 | Basic data manipulation | TODO | |
| E121 | Basic cursor support | Not Implemented | |
| E141 | Basic integrity constraints | Partial | |
| E151 | Transaction support | Supported | |
| T151 | DISTINCT predicate | Not Implemented | |

### SQL:2016 New Features

| Feature ID | Description | Status |
|------------|-------------|--------|
| T803 | String-based JSON | Not Implemented |
| T811 | JSON_OBJECT | Not Implemented |
| T821 | JSON_EXISTS | Not Implemented |
| T823 | JSON_VALUE | Not Implemented |
| T827 | JSON_TABLE | Not Implemented |
| - | Row Pattern Recognition | Not Implemented |
| - | Polymorphic Table Functions | Not Implemented |
| - | LISTAGG | Not Implemented |

## Adding New Tests

1. **Find or create the feature module** in the appropriate part directory
2. **Add test functions** using the conformance macros:

```rust
/// E011-01: INTEGER data type
#[test]
fn e011_01_integer_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x INTEGER)",
        "E011-01",
        "INTEGER data type"
    );
}
```

3. **For unimplemented features**, use `assert_not_implemented!`:

```rust
/// T151: DISTINCT predicate (not yet implemented)
#[test]
fn t151_distinct_predicate() {
    assert_not_implemented!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM b",
        "T151",
        "DISTINCT predicate"
    );
}
```

4. **Update this README** with the feature status

## Feature Naming Convention

- Feature files: `{feature_id}_{description}.rs` (e.g., `e011_numeric_types.rs`)
- Test functions: `{feature_id}_{subfeature}_{test_name}` (e.g., `e011_01_integer_column`)
- Modules for feature groups: `{feature_id}_{subfeature}` (e.g., `mod e011_01`)

## References

- [SQL:2016 Wikipedia](https://en.wikipedia.org/wiki/SQL:2016)
- [PostgreSQL SQL Conformance](https://www.postgresql.org/docs/current/features.html)
- [SQL 2003 Core Features](https://ronsavage.github.io/SQL/sql-2003-core-features.html)
- [SQL 2003 Non-Core Features](https://ronsavage.github.io/SQL/sql-2003-noncore-features.html)

## Contributing

When implementing a new SQL feature:

1. Check if there's an existing `assert_not_implemented!` test for it
2. If so, the test will start failing when you implement the feature
3. Update the test to use `assert_feature_supported!` or `assert_plans!`
4. Update the status in this README

This ensures we maintain accurate conformance tracking as features are added.
