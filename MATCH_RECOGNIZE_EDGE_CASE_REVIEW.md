# MATCH_RECOGNIZE Edge Cases & Error Handling Review

## Review Date
2025-12-27

## Reviewer
Reviewer B - Edge Cases & Error Handling

## Files Reviewed
- `/home/jeff/repos/datafusion/datafusion/sql/src/relation/mod.rs` (lines 83-661)
- `/home/jeff/repos/datafusion/datafusion/expr/src/logical_plan/plan.rs` (lines 256-408)
- `/home/jeff/repos/datafusion/datafusion/sql/tests/part2_foundation/r010_row_pattern_recognition.rs`
- `/home/jeff/repos/datafusion/datafusion/sql/tests/match_recognize_edge_cases.rs` (custom test file)

---

## 1. Empty PATTERN Edge Case

### Status: **HANDLED CORRECTLY**

**Test Result:**
```
Empty pattern result: Err("Parse error: sql parser error: Expected: identifier, found: ) at Line: 1, Column: 55")
```

**Analysis:**
- Empty PATTERN `()` is rejected **at parse time** by sqlparser
- This is the correct layer to reject this - it's a syntax error, not a semantic error
- No special handling needed in DataFusion's planner

**Conclusion:** ✅ No issues found

---

## 2. Complex Nested Patterns: `((A | B)* C)+`

### Status: **HANDLED CORRECTLY**

**Test Results:**
All complex nesting patterns parse and plan successfully:
- `(((A | B)* C)+)` - nested alternation with quantifiers
- `((((A | B) | (C | D)) | ((E | F) | (G | H)))+)` - deeply nested alternation
- `(((((((((A)))))))))` - deep grouping nesting

**Code Analysis:**
```rust
// From relation/mod.rs lines 545-589
fn convert_pattern(
    pat: MatchRecognizePattern,
    convert_symbol: &impl Fn(MatchRecognizeSymbol) -> PatternSymbol,
    convert_quantifier: &impl Fn(SqlRepetitionQuantifier) -> RepetitionQuantifier,
) -> Pattern {
    match pat {
        MatchRecognizePattern::Concat(pats) => Pattern::Concat(...),
        MatchRecognizePattern::Group(pat) => Pattern::Group(Box::new(...)),
        MatchRecognizePattern::Alternation(pats) => Pattern::Alternation(...),
        MatchRecognizePattern::Repetition(pat, quant) => Pattern::Repetition(...),
        ...
    }
}
```

**Analysis:**
- Recursive conversion handles arbitrary nesting depth
- No stack overflow protection visible, but Rust's default stack should handle reasonable nesting
- Pattern complexity is limited by sqlparser's parsing capabilities

**Conclusion:** ✅ No issues found

---

## 3. PERMUTE Patterns

### Status: **HANDLED CORRECTLY**

**Test Result:**
```sql
PATTERN (PERMUTE(A, B, C))
```
✅ Parses and plans successfully

**Code Analysis:**
```rust
// From relation/mod.rs lines 120-129
MatchRecognizePattern::Permute(syms) => {
    for sym in syms {
        if let sqlparser::ast::MatchRecognizeSymbol::Named(ident) = sym {
            let name = ident.value.clone();
            if !symbols.contains(&name) {
                symbols.push(name);
            }
        }
    }
}

// From relation/mod.rs lines 558-560
MatchRecognizePattern::Permute(syms) => {
    Pattern::Permute(syms.into_iter().map(convert_symbol).collect())
}
```

**Analysis:**
- PERMUTE pattern is recognized and converted correctly
- Symbols are extracted for schema augmentation
- No validation of PERMUTE-specific semantics (e.g., ensuring all symbols are unique)

**Potential Issue:** ⚠️ **Minor** - No validation that PERMUTE symbols are unique or that PERMUTE doesn't contain Start/End anchors (which would be semantically invalid)

**Conclusion:** ⚠️ Minor improvement possible

---

## 4. SQL Injection & Security Concerns

### Status: **NO SECURITY ISSUES**

**Analysis:**

#### 4.1 Identifier Handling
All identifiers go through the `ident_normalizer`:
```rust
// From relation/mod.rs lines 459-465
let mut pattern_var_names: Vec<String> = raw_pattern_symbols
    .into_iter()
    .map(|s| {
        use sqlparser::ast::Ident;
        self.ident_normalizer.normalize(Ident::new(s))
    })
    .collect();
```

#### 4.2 Expression Planning
All SQL expressions are converted through `sql_to_expr`:
```rust
// From relation/mod.rs lines 486-495
let mut converted_expr = self.sql_to_expr(expr, &augmented_schema, planner_context)?;
converted_expr = self.strip_pattern_var_qualifiers(converted_expr, &pattern_var_names);
```

#### 4.3 No String Interpolation
- All values flow through proper AST structures
- No raw SQL concatenation
- Pattern variables are treated as identifiers, not string literals

**Test Results:**
```rust
// SQL injection attempt via identifier
MEASURES A.value AS "'; DROP TABLE t; --"
```
✅ Handled safely - the string becomes a column name, not executed SQL

**Conclusion:** ✅ No SQL injection vectors found

---

## 5. Pattern Quantifier Edge Cases

### 5.1 Zero Range Quantifier: `{0,0}`

**Status:** ⚠️ **ISSUE - NO VALIDATION**

**Test Result:**
```
Zero range quantifier result: Ok(())
```

**Analysis:**
- `A{0,0}` parses successfully
- This pattern can never match anything (requires exactly 0 occurrences)
- No validation in `convert_quantifier` or `try_new`

**Code:**
```rust
// From relation/mod.rs lines 535-543
let convert_quantifier = |q: SqlRepetitionQuantifier| match q {
    SqlRepetitionQuantifier::Range(n, m) => RepetitionQuantifier::Range(n, m),
    // No validation of n vs m relationship
    ...
};
```

**Recommendation:** Add validation in `MatchRecognize::try_new` or `convert_quantifier` to reject:
- `{0,0}` - semantically useless
- `{n,m}` where `n > m` - invalid range

---

### 5.2 Invalid Range Quantifier: `{5,2}`

**Status:** ⚠️ **ISSUE - NO VALIDATION**

**Test Result:**
```
Invalid range quantifier result: Ok(())
```

**Analysis:**
- `A{5,2}` parses successfully (min=5, max=2)
- This is semantically invalid (cannot have minimum > maximum)
- Would likely cause runtime errors or unexpected behavior

**Recommendation:** Add validation to reject `{n,m}` where `n > m`

---

### 5.3 Very Large Quantifiers

**Test Result:**
```sql
PATTERN (A{1000,5000})
```
✅ Parses successfully

**Analysis:**
- No upper bound validation on quantifier values
- Could potentially cause:
  - Memory issues with very large repetitions
  - Performance degradation
  - Stack overflow in recursive pattern matching

**Recommendation:** Consider adding reasonable upper bounds (e.g., 10000) to prevent abuse

---

## 6. Pattern Variables Without DEFINE

### Status: **HANDLED CORRECTLY** (by design)

**Test Case:**
```sql
PATTERN (STRT DOWN+ UP+)
DEFINE
    DOWN AS value < 100,
    UP AS value > 100
-- STRT has no DEFINE
```

✅ This is **valid SQL** according to the standard. A pattern variable without a DEFINE clause is treated as always matching.

**Code Analysis:**
```rust
// From relation/mod.rs lines 455-474
// Collect all pattern variable names from both PATTERN and DEFINE clauses
let raw_pattern_symbols = extract_pattern_symbols(&pattern);

// Also add symbols from DEFINE clause
for SymbolDefinition { symbol, .. } in &symbols {
    let name = self.ident_normalizer.normalize(symbol.clone());
    if !pattern_var_names.contains(&name) {
        pattern_var_names.push(name);
    }
}
```

**Conclusion:** ✅ Correctly implements SQL:2016 semantics

---

## 7. Edge Cases Summary Table

| Edge Case | Status | Severity | Issue Found |
|-----------|--------|----------|-------------|
| Empty PATTERN `()` | ✅ Handled | N/A | Rejected by parser |
| Complex nesting `((A\|B)* C)+` | ✅ Handled | N/A | Works correctly |
| PERMUTE patterns | ⚠️ Partial | Minor | No validation of uniqueness |
| SQL injection | ✅ Secure | N/A | No vectors found |
| Zero range `{0,0}` | ❌ Not validated | Medium | Accepted but useless |
| Invalid range `{5,2}` | ❌ Not validated | High | Accepted but invalid |
| Very large quantifiers | ⚠️ No limits | Low | Could cause DoS |
| Pattern vars without DEFINE | ✅ Handled | N/A | Correct per spec |
| Deep nesting | ⚠️ No limits | Low | Could cause stack overflow |
| Special characters in identifiers | ✅ Handled | N/A | Works correctly |
| Column name collisions | ✅ Handled | N/A | Schema handles it |
| NULL values in DEFINE | ✅ Handled | N/A | Valid SQL |

---

## 8. Recommended Fixes

### High Priority

1. **Validate Range Quantifiers**
   ```rust
   // In convert_quantifier or MatchRecognize::try_new
   SqlRepetitionQuantifier::Range(n, m) => {
       if n > m {
           return plan_err!("Invalid range quantifier {{{}:{}}}: minimum cannot exceed maximum", n, m);
       }
       if n == 0 && m == 0 {
           return plan_err!("Range quantifier {{0,0}} can never match");
       }
       RepetitionQuantifier::Range(n, m)
   }
   ```

### Medium Priority

2. **Add Reasonable Upper Bounds**
   ```rust
   const MAX_QUANTIFIER: u32 = 100000;

   SqlRepetitionQuantifier::Exactly(n) |
   SqlRepetitionQuantifier::AtLeast(n) |
   SqlRepetitionQuantifier::AtMost(n) => {
       if n > MAX_QUANTIFIER {
           return plan_err!("Quantifier value {} exceeds maximum {}", n, MAX_QUANTIFIER);
       }
       ...
   }
   ```

### Low Priority

3. **Validate PERMUTE Patterns**
   ```rust
   Pattern::Permute(syms) => {
       // Check for duplicates
       let mut seen = HashSet::new();
       for sym in &syms {
           if let PatternSymbol::Named(name) = sym {
               if !seen.insert(name) {
                   return plan_err!("Duplicate symbol '{}' in PERMUTE pattern", name);
               }
           }
       }
       ...
   }
   ```

---

## 9. Testing Recommendations

### Additional Tests Needed

1. **Quantifier Validation Tests**
   - Test `{0,0}` rejection
   - Test `{5,2}` rejection
   - Test very large values (e.g., `{2147483647}`)
   - Test negative values (if parser allows)

2. **Stress Tests**
   - Very deep nesting (100+ levels)
   - Very wide alternation (100+ branches)
   - Large PERMUTE patterns (50+ symbols)

3. **Security Tests**
   - Malformed UTF-8 in identifiers
   - Very long identifier names (>1MB)
   - Patterns designed to cause exponential backtracking

---

## 10. Overall Assessment

### Strengths
- ✅ SQL injection is properly prevented through AST-based planning
- ✅ Complex nested patterns are handled correctly
- ✅ Identifier normalization is applied consistently
- ✅ Pattern variable scoping is implemented correctly

### Weaknesses
- ❌ No validation of quantifier range validity (n ≤ m)
- ⚠️ No upper bounds on quantifier values
- ⚠️ No validation of PERMUTE pattern uniqueness
- ⚠️ No protection against extremely deep nesting

### Risk Level
**MEDIUM** - The main issues are around quantifier validation which could lead to confusing error messages or runtime failures, but no security vulnerabilities were found.

### Recommendation
**APPROVE WITH REQUIRED FIXES** - The high-priority quantifier validation should be added before merging to production. The other issues can be addressed in follow-up work.

---

## Test Coverage

Created comprehensive edge case tests in:
- `/home/jeff/repos/datafusion/datafusion/sql/tests/match_recognize_edge_cases.rs`

All 16 tests pass, demonstrating that:
- Complex patterns are supported
- Parsing works correctly
- No crashes or panics occur

However, semantic validation is still needed for invalid quantifier ranges.
