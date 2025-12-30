// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL:2016 Feature F051 - Basic date and time
//! SQL:2016 Feature F052 - Intervals and datetime arithmetic
//!
//! ISO/IEC 9075-2:2016 Section 6.1 (Data types)
//!
//! This module tests basic datetime and interval support required by Core SQL.
//!
//! # F051: Basic date and time
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F051-01 | DATE data type (including DATE literal) | Partial |
//! | F051-02 | TIME data type with fractional seconds precision | Partial |
//! | F051-03 | TIMESTAMP data type with fractional seconds precision | Partial |
//! | F051-04 | Comparison predicate on DATE, TIME, TIMESTAMP | Supported |
//! | F051-05 | Explicit CAST between datetime types and character types | Supported |
//! | F051-06 | CURRENT_DATE | Supported |
//! | F051-07 | LOCALTIME | Partial |
//! | F051-08 | LOCALTIMESTAMP | Supported |
//!
//! # F052: Intervals and datetime arithmetic
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F052 | INTERVAL data type | Supported |
//! | F052 | INTERVAL literals | Supported |
//! | F052 | Date/time arithmetic | Supported |
//! | F052 | EXTRACT function | Supported |
//!
//! # F411: Time zone specification
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F411-01 | TIMESTAMP WITH TIME ZONE | Partial |
//! | F411-02 | TIME WITH TIME ZONE | Partial |
//! | F411-03 | AT TIME ZONE clause | Partial |
//!
//! F051 is a CORE feature (mandatory for SQL:2016 conformance).

use crate::{assert_feature_supported, assert_plans};

// ============================================================================
// F051-01: DATE data type (including DATE literal)
// ============================================================================

/// F051-01: DATE data type in column definition
#[test]
fn f051_01_date_column() {
    assert_feature_supported!(
        "CREATE TABLE t (birth_date DATE)",
        "F051-01",
        "DATE data type in column definition"
    );
}

/// F051-01: DATE literal syntax
#[test]
fn f051_01_date_literal() {
    assert_feature_supported!(
        "SELECT DATE '2024-01-15'",
        "F051-01",
        "DATE literal"
    );
}

/// F051-01: DATE literal in WHERE clause
#[test]
fn f051_01_date_literal_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col = DATE '2024-01-15'",
        "F051-01",
        "DATE literal in WHERE clause"
    );
}

/// F051-01: DATE literal with different formats
#[test]
fn f051_01_date_literal_various() {
    assert_feature_supported!(
        "SELECT DATE '2024-12-31'",
        "F051-01",
        "DATE literal end of year"
    );
}

/// F051-01: DATE in SELECT list
#[test]
fn f051_01_date_select() {
    assert_feature_supported!(
        "SELECT date_col FROM datetime_types",
        "F051-01",
        "DATE column in SELECT"
    );
}

/// F051-01: DATE in ORDER BY
#[test]
fn f051_01_date_order_by() {
    assert_feature_supported!(
        "SELECT date_col FROM datetime_types ORDER BY date_col",
        "F051-01",
        "DATE in ORDER BY"
    );
}

/// F051-01: Multiple DATE literals
#[test]
fn f051_01_multiple_date_literals() {
    assert_feature_supported!(
        "SELECT DATE '2024-01-01', DATE '2024-12-31'",
        "F051-01",
        "Multiple DATE literals"
    );
}

// ============================================================================
// F051-02: TIME data type with fractional seconds precision
// ============================================================================

/// F051-02: TIME data type in column definition
#[test]
fn f051_02_time_column() {
    assert_feature_supported!(
        "CREATE TABLE t (start_time TIME)",
        "F051-02",
        "TIME data type in column definition"
    );
}

/// F051-02: TIME with precision
#[test]
fn f051_02_time_with_precision() {
    assert_feature_supported!(
        "CREATE TABLE t (start_time TIME(6))",
        "F051-02",
        "TIME with fractional seconds precision"
    );
}

/// F051-02: TIME literal syntax
#[test]
fn f051_02_time_literal() {
    assert_feature_supported!(
        "SELECT TIME '12:30:45'",
        "F051-02",
        "TIME literal"
    );
}

/// F051-02: TIME literal with fractional seconds
#[test]
fn f051_02_time_literal_fractional() {
    assert_feature_supported!(
        "SELECT TIME '12:30:45.123456'",
        "F051-02",
        "TIME literal with fractional seconds"
    );
}

/// F051-02: TIME in WHERE clause
#[test]
fn f051_02_time_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE time_col > TIME '09:00:00'",
        "F051-02",
        "TIME in WHERE clause"
    );
}

/// F051-02: TIME in SELECT list
#[test]
fn f051_02_time_select() {
    assert_feature_supported!(
        "SELECT time_col FROM datetime_types",
        "F051-02",
        "TIME column in SELECT"
    );
}

/// F051-02: TIME in ORDER BY
#[test]
fn f051_02_time_order_by() {
    assert_feature_supported!(
        "SELECT time_col FROM datetime_types ORDER BY time_col",
        "F051-02",
        "TIME in ORDER BY"
    );
}

// ============================================================================
// F051-03: TIMESTAMP data type with fractional seconds precision
// ============================================================================

/// F051-03: TIMESTAMP data type in column definition
#[test]
fn f051_03_timestamp_column() {
    assert_feature_supported!(
        "CREATE TABLE t (created_at TIMESTAMP)",
        "F051-03",
        "TIMESTAMP data type in column definition"
    );
}

/// F051-03: TIMESTAMP with precision 0
#[test]
fn f051_03_timestamp_precision_0() {
    assert_feature_supported!(
        "CREATE TABLE t (created_at TIMESTAMP(0))",
        "F051-03",
        "TIMESTAMP with precision 0"
    );
}

/// F051-03: TIMESTAMP with precision 6
#[test]
fn f051_03_timestamp_precision_6() {
    assert_feature_supported!(
        "CREATE TABLE t (created_at TIMESTAMP(6))",
        "F051-03",
        "TIMESTAMP with precision 6"
    );
}

/// F051-03: TIMESTAMP literal syntax
#[test]
fn f051_03_timestamp_literal() {
    assert_feature_supported!(
        "SELECT TIMESTAMP '2024-01-15 12:30:45'",
        "F051-03",
        "TIMESTAMP literal"
    );
}

/// F051-03: TIMESTAMP literal with fractional seconds
#[test]
fn f051_03_timestamp_literal_fractional() {
    assert_feature_supported!(
        "SELECT TIMESTAMP '2024-01-15 12:30:45.123456'",
        "F051-03",
        "TIMESTAMP literal with fractional seconds"
    );
}

/// F051-03: TIMESTAMP in WHERE clause
#[test]
fn f051_03_timestamp_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE timestamp_col > TIMESTAMP '2024-01-01 00:00:00'",
        "F051-03",
        "TIMESTAMP in WHERE clause"
    );
}

/// F051-03: TIMESTAMP in SELECT list
#[test]
fn f051_03_timestamp_select() {
    assert_feature_supported!(
        "SELECT timestamp_col FROM datetime_types",
        "F051-03",
        "TIMESTAMP column in SELECT"
    );
}

/// F051-03: TIMESTAMP in ORDER BY
#[test]
fn f051_03_timestamp_order_by() {
    assert_feature_supported!(
        "SELECT timestamp_col FROM datetime_types ORDER BY timestamp_col DESC",
        "F051-03",
        "TIMESTAMP in ORDER BY"
    );
}

// ============================================================================
// F051-04: Comparison predicate on DATE, TIME, TIMESTAMP
// ============================================================================

/// F051-04: DATE equality comparison
#[test]
fn f051_04_date_equality() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col = DATE '2024-01-15'",
        "F051-04",
        "DATE equality comparison"
    );
}

/// F051-04: DATE less than comparison
#[test]
fn f051_04_date_less_than() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col < DATE '2024-12-31'",
        "F051-04",
        "DATE less than comparison"
    );
}

/// F051-04: DATE greater than or equal comparison
#[test]
fn f051_04_date_gte() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col >= DATE '2024-01-01'",
        "F051-04",
        "DATE greater than or equal comparison"
    );
}

/// F051-04: DATE between comparison
#[test]
fn f051_04_date_between() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'",
        "F051-04",
        "DATE BETWEEN comparison"
    );
}

/// F051-04: TIME equality comparison
#[test]
fn f051_04_time_equality() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE time_col = TIME '12:00:00'",
        "F051-04",
        "TIME equality comparison"
    );
}

/// F051-04: TIME less than comparison
#[test]
fn f051_04_time_less_than() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE time_col < TIME '17:00:00'",
        "F051-04",
        "TIME less than comparison"
    );
}

/// F051-04: TIMESTAMP equality comparison
#[test]
fn f051_04_timestamp_equality() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE timestamp_col = TIMESTAMP '2024-01-15 12:00:00'",
        "F051-04",
        "TIMESTAMP equality comparison"
    );
}

/// F051-04: TIMESTAMP range comparison
#[test]
fn f051_04_timestamp_range() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE timestamp_col > TIMESTAMP '2024-01-01 00:00:00' AND timestamp_col < TIMESTAMP '2024-12-31 23:59:59'",
        "F051-04",
        "TIMESTAMP range comparison"
    );
}

/// F051-04: Datetime column comparison
#[test]
fn f051_04_column_comparison() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE birth_date < CURRENT_TIMESTAMP",
        "F051-04",
        "Datetime column to function comparison"
    );
}

/// F051-04: Datetime inequality
#[test]
fn f051_04_datetime_inequality() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col <> DATE '2024-01-01'",
        "F051-04",
        "DATE inequality comparison"
    );
}

// ============================================================================
// F051-05: Explicit CAST between datetime types and character types
// ============================================================================

/// F051-05: CAST DATE to VARCHAR
#[test]
fn f051_05_cast_date_to_varchar() {
    assert_feature_supported!(
        "SELECT CAST(DATE '2024-01-15' AS VARCHAR)",
        "F051-05",
        "CAST DATE to VARCHAR"
    );
}

/// F051-05: CAST VARCHAR to DATE
#[test]
fn f051_05_cast_varchar_to_date() {
    assert_feature_supported!(
        "SELECT CAST('2024-01-15' AS DATE)",
        "F051-05",
        "CAST VARCHAR to DATE"
    );
}

/// F051-05: CAST TIME to VARCHAR
#[test]
fn f051_05_cast_time_to_varchar() {
    assert_feature_supported!(
        "SELECT CAST(TIME '12:30:45' AS VARCHAR)",
        "F051-05",
        "CAST TIME to VARCHAR"
    );
}

/// F051-05: CAST VARCHAR to TIME
#[test]
fn f051_05_cast_varchar_to_time() {
    assert_feature_supported!(
        "SELECT CAST('12:30:45' AS TIME)",
        "F051-05",
        "CAST VARCHAR to TIME"
    );
}

/// F051-05: CAST TIMESTAMP to VARCHAR
#[test]
fn f051_05_cast_timestamp_to_varchar() {
    assert_feature_supported!(
        "SELECT CAST(TIMESTAMP '2024-01-15 12:30:45' AS VARCHAR)",
        "F051-05",
        "CAST TIMESTAMP to VARCHAR"
    );
}

/// F051-05: CAST VARCHAR to TIMESTAMP
#[test]
fn f051_05_cast_varchar_to_timestamp() {
    assert_feature_supported!(
        "SELECT CAST('2024-01-15 12:30:45' AS TIMESTAMP)",
        "F051-05",
        "CAST VARCHAR to TIMESTAMP"
    );
}

/// F051-05: CAST DATE to TIMESTAMP
#[test]
fn f051_05_cast_date_to_timestamp() {
    assert_feature_supported!(
        "SELECT CAST(DATE '2024-01-15' AS TIMESTAMP)",
        "F051-05",
        "CAST DATE to TIMESTAMP"
    );
}

/// F051-05: CAST TIMESTAMP to DATE
#[test]
fn f051_05_cast_timestamp_to_date() {
    assert_feature_supported!(
        "SELECT CAST(TIMESTAMP '2024-01-15 12:30:45' AS DATE)",
        "F051-05",
        "CAST TIMESTAMP to DATE"
    );
}

/// F051-05: CAST TIMESTAMP to TIME
#[test]
fn f051_05_cast_timestamp_to_time() {
    assert_feature_supported!(
        "SELECT CAST(TIMESTAMP '2024-01-15 12:30:45' AS TIME)",
        "F051-05",
        "CAST TIMESTAMP to TIME"
    );
}

/// F051-05: CAST with column
#[test]
fn f051_05_cast_column() {
    assert_feature_supported!(
        "SELECT CAST(date_col AS VARCHAR) FROM datetime_types",
        "F051-05",
        "CAST DATE column to VARCHAR"
    );
}

// ============================================================================
// F051-06: CURRENT_DATE
// ============================================================================

/// F051-06: CURRENT_DATE function
#[test]
fn f051_06_current_date() {
    assert_feature_supported!(
        "SELECT CURRENT_DATE",
        "F051-06",
        "CURRENT_DATE function"
    );
}

/// F051-06: CURRENT_DATE in WHERE clause
#[test]
fn f051_06_current_date_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col = CURRENT_DATE",
        "F051-06",
        "CURRENT_DATE in WHERE clause"
    );
}

/// F051-06: CURRENT_DATE comparison
#[test]
fn f051_06_current_date_comparison() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col < CURRENT_DATE",
        "F051-06",
        "CURRENT_DATE in comparison"
    );
}

/// F051-06: CURRENT_DATE in SELECT list
#[test]
fn f051_06_current_date_select() {
    assert_feature_supported!(
        "SELECT date_col, CURRENT_DATE FROM datetime_types",
        "F051-06",
        "CURRENT_DATE in SELECT list"
    );
}

// ============================================================================
// F051-07: LOCALTIME
// ============================================================================

/// F051-07: LOCALTIME function
#[test]
fn f051_07_localtime() {
    assert_feature_supported!(
        "SELECT LOCALTIME",
        "F051-07",
        "LOCALTIME function"
    );
}

/// F051-07: LOCALTIME with precision
#[test]
fn f051_07_localtime_precision() {
    assert_feature_supported!(
        "SELECT LOCALTIME(3)",
        "F051-07",
        "LOCALTIME with precision"
    );
}

/// F051-07: LOCALTIME in WHERE clause
#[test]
fn f051_07_localtime_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE time_col < LOCALTIME",
        "F051-07",
        "LOCALTIME in WHERE clause"
    );
}

/// F051-07: LOCALTIME in SELECT list
#[test]
fn f051_07_localtime_select() {
    assert_feature_supported!(
        "SELECT time_col, LOCALTIME FROM datetime_types",
        "F051-07",
        "LOCALTIME in SELECT list"
    );
}

// ============================================================================
// F051-08: LOCALTIMESTAMP
// ============================================================================

/// F051-08: LOCALTIMESTAMP function
#[test]
fn f051_08_localtimestamp() {
    assert_feature_supported!(
        "SELECT LOCALTIMESTAMP",
        "F051-08",
        "LOCALTIMESTAMP function"
    );
}

/// F051-08: LOCALTIMESTAMP with precision
#[test]
fn f051_08_localtimestamp_precision() {
    assert_feature_supported!(
        "SELECT LOCALTIMESTAMP(6)",
        "F051-08",
        "LOCALTIMESTAMP with precision"
    );
}

/// F051-08: LOCALTIMESTAMP in WHERE clause
#[test]
fn f051_08_localtimestamp_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE timestamp_col < LOCALTIMESTAMP",
        "F051-08",
        "LOCALTIMESTAMP in WHERE clause"
    );
}

/// F051-08: LOCALTIMESTAMP in SELECT list
#[test]
fn f051_08_localtimestamp_select() {
    assert_feature_supported!(
        "SELECT timestamp_col, LOCALTIMESTAMP FROM datetime_types",
        "F051-08",
        "LOCALTIMESTAMP in SELECT list"
    );
}

// ============================================================================
// F052: INTERVAL data type and literals
// ============================================================================

/// F052: INTERVAL YEAR data type
#[test]
fn f052_interval_year_type() {
    assert_feature_supported!(
        "CREATE TABLE t (duration INTERVAL YEAR)",
        "F052",
        "INTERVAL YEAR data type"
    );
}

/// F052: INTERVAL MONTH data type
#[test]
fn f052_interval_month_type() {
    assert_feature_supported!(
        "CREATE TABLE t (duration INTERVAL MONTH)",
        "F052",
        "INTERVAL MONTH data type"
    );
}

/// F052: INTERVAL DAY data type
#[test]
fn f052_interval_day_type() {
    assert_feature_supported!(
        "CREATE TABLE t (duration INTERVAL DAY)",
        "F052",
        "INTERVAL DAY data type"
    );
}

/// F052: INTERVAL HOUR data type
#[test]
fn f052_interval_hour_type() {
    assert_feature_supported!(
        "CREATE TABLE t (duration INTERVAL HOUR)",
        "F052",
        "INTERVAL HOUR data type"
    );
}

/// F052: INTERVAL literal - YEAR
#[test]
fn f052_interval_literal_year() {
    assert_feature_supported!(
        "SELECT INTERVAL '1' YEAR",
        "F052",
        "INTERVAL literal YEAR"
    );
}

/// F052: INTERVAL literal - MONTH
#[test]
fn f052_interval_literal_month() {
    assert_feature_supported!(
        "SELECT INTERVAL '3' MONTH",
        "F052",
        "INTERVAL literal MONTH"
    );
}

/// F052: INTERVAL literal - DAY
#[test]
fn f052_interval_literal_day() {
    assert_feature_supported!(
        "SELECT INTERVAL '7' DAY",
        "F052",
        "INTERVAL literal DAY"
    );
}

/// F052: INTERVAL literal - HOUR
#[test]
fn f052_interval_literal_hour() {
    assert_feature_supported!(
        "SELECT INTERVAL '2' HOUR",
        "F052",
        "INTERVAL literal HOUR"
    );
}

/// F052: INTERVAL literal - MINUTE
#[test]
fn f052_interval_literal_minute() {
    assert_feature_supported!(
        "SELECT INTERVAL '30' MINUTE",
        "F052",
        "INTERVAL literal MINUTE"
    );
}

/// F052: INTERVAL literal - SECOND
#[test]
fn f052_interval_literal_second() {
    assert_feature_supported!(
        "SELECT INTERVAL '45' SECOND",
        "F052",
        "INTERVAL literal SECOND"
    );
}

/// F052: INTERVAL literal - compound YEAR TO MONTH
#[test]
fn f052_interval_literal_year_to_month() {
    assert_feature_supported!(
        "SELECT INTERVAL '1-6' YEAR TO MONTH",
        "F052",
        "INTERVAL literal YEAR TO MONTH"
    );
}

/// F052: INTERVAL literal - compound DAY TO HOUR
#[test]
fn f052_interval_literal_day_to_hour() {
    assert_feature_supported!(
        "SELECT INTERVAL '2 3' DAY TO HOUR",
        "F052",
        "INTERVAL literal DAY TO HOUR"
    );
}

/// F052: INTERVAL literal - compound DAY TO SECOND
#[test]
fn f052_interval_literal_day_to_second() {
    assert_feature_supported!(
        "SELECT INTERVAL '1 12:30:45' DAY TO SECOND",
        "F052",
        "INTERVAL literal DAY TO SECOND"
    );
}

/// F052: INTERVAL literal - compound HOUR TO MINUTE
#[test]
fn f052_interval_literal_hour_to_minute() {
    assert_feature_supported!(
        "SELECT INTERVAL '2:30' HOUR TO MINUTE",
        "F052",
        "INTERVAL literal HOUR TO MINUTE"
    );
}

/// F052: INTERVAL literal - SQL standard string syntax
#[test]
fn f052_interval_literal_string_syntax() {
    assert_feature_supported!(
        "SELECT INTERVAL '1 year 2 months'",
        "F052",
        "INTERVAL literal with string syntax"
    );
}

/// F052: INTERVAL literal - PostgreSQL-style syntax
#[test]
fn f052_interval_literal_postgres_style() {
    assert_feature_supported!(
        "SELECT INTERVAL '3 days 4 hours'",
        "F052",
        "INTERVAL literal PostgreSQL style"
    );
}

// ============================================================================
// F052: Date/time arithmetic
// ============================================================================

/// F052: DATE plus INTERVAL
#[test]
fn f052_date_plus_interval() {
    assert_feature_supported!(
        "SELECT DATE '2024-01-15' + INTERVAL '1' DAY",
        "F052",
        "DATE plus INTERVAL"
    );
}

/// F052: DATE minus INTERVAL
#[test]
fn f052_date_minus_interval() {
    assert_feature_supported!(
        "SELECT DATE '2024-01-15' - INTERVAL '7' DAY",
        "F052",
        "DATE minus INTERVAL"
    );
}

/// F052: TIMESTAMP plus INTERVAL
#[test]
fn f052_timestamp_plus_interval() {
    assert_feature_supported!(
        "SELECT TIMESTAMP '2024-01-15 12:00:00' + INTERVAL '2' HOUR",
        "F052",
        "TIMESTAMP plus INTERVAL"
    );
}

/// F052: TIMESTAMP minus INTERVAL
#[test]
fn f052_timestamp_minus_interval() {
    assert_feature_supported!(
        "SELECT TIMESTAMP '2024-01-15 12:00:00' - INTERVAL '30' MINUTE",
        "F052",
        "TIMESTAMP minus INTERVAL"
    );
}

/// F052: TIME plus INTERVAL
#[test]
fn f052_time_plus_interval() {
    assert_feature_supported!(
        "SELECT TIME '12:00:00' + INTERVAL '1' HOUR",
        "F052",
        "TIME plus INTERVAL"
    );
}

/// F052: TIME minus INTERVAL
#[test]
fn f052_time_minus_interval() {
    assert_feature_supported!(
        "SELECT TIME '12:00:00' - INTERVAL '15' MINUTE",
        "F052",
        "TIME minus INTERVAL"
    );
}

/// F052: Column date arithmetic
#[test]
fn f052_column_date_arithmetic() {
    assert_feature_supported!(
        "SELECT date_col + INTERVAL '1' MONTH FROM datetime_types",
        "F052",
        "Column DATE arithmetic"
    );
}

/// F052: Column timestamp arithmetic
#[test]
fn f052_column_timestamp_arithmetic() {
    assert_feature_supported!(
        "SELECT timestamp_col - INTERVAL '1' DAY FROM datetime_types",
        "F052",
        "Column TIMESTAMP arithmetic"
    );
}

/// F052: Date arithmetic in WHERE clause
#[test]
fn f052_date_arithmetic_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE date_col + INTERVAL '7' DAY > CURRENT_DATE",
        "F052",
        "Date arithmetic in WHERE clause"
    );
}

/// F052: Complex interval arithmetic
#[test]
fn f052_complex_interval_arithmetic() {
    assert_feature_supported!(
        "SELECT DATE '2024-01-15' + INTERVAL '1' YEAR + INTERVAL '2' MONTH",
        "F052",
        "Complex interval arithmetic"
    );
}

// ============================================================================
// F052: INTERVAL arithmetic
// ============================================================================

/// F052: INTERVAL plus INTERVAL
#[test]
fn f052_interval_plus_interval() {
    assert_feature_supported!(
        "SELECT INTERVAL '1' DAY + INTERVAL '2' DAY",
        "F052",
        "INTERVAL plus INTERVAL"
    );
}

/// F052: INTERVAL minus INTERVAL
#[test]
fn f052_interval_minus_interval() {
    assert_feature_supported!(
        "SELECT INTERVAL '5' HOUR - INTERVAL '2' HOUR",
        "F052",
        "INTERVAL minus INTERVAL"
    );
}

/// F052: INTERVAL multiplication
#[test]
fn f052_interval_multiplication() {
    assert_feature_supported!(
        "SELECT INTERVAL '1' DAY * 7",
        "F052",
        "INTERVAL multiplication"
    );
}

/// F052: INTERVAL division
#[test]
fn f052_interval_division() {
    assert_feature_supported!(
        "SELECT INTERVAL '10' DAY / 2",
        "F052",
        "INTERVAL division"
    );
}

/// F052: INTERVAL column arithmetic
#[test]
fn f052_interval_column_arithmetic() {
    assert_feature_supported!(
        "SELECT interval_col + INTERVAL '1' DAY FROM datetime_types",
        "F052",
        "INTERVAL column arithmetic"
    );
}

// ============================================================================
// F052: EXTRACT function
// ============================================================================

/// F052: EXTRACT YEAR from DATE
#[test]
fn f052_extract_year_from_date() {
    assert_feature_supported!(
        "SELECT EXTRACT(YEAR FROM DATE '2024-01-15')",
        "F052",
        "EXTRACT YEAR from DATE"
    );
}

/// F052: EXTRACT MONTH from DATE
#[test]
fn f052_extract_month_from_date() {
    assert_feature_supported!(
        "SELECT EXTRACT(MONTH FROM DATE '2024-01-15')",
        "F052",
        "EXTRACT MONTH from DATE"
    );
}

/// F052: EXTRACT DAY from DATE
#[test]
fn f052_extract_day_from_date() {
    assert_feature_supported!(
        "SELECT EXTRACT(DAY FROM DATE '2024-01-15')",
        "F052",
        "EXTRACT DAY from DATE"
    );
}

/// F052: EXTRACT HOUR from TIME
#[test]
fn f052_extract_hour_from_time() {
    assert_feature_supported!(
        "SELECT EXTRACT(HOUR FROM TIME '12:30:45')",
        "F052",
        "EXTRACT HOUR from TIME"
    );
}

/// F052: EXTRACT MINUTE from TIME
#[test]
fn f052_extract_minute_from_time() {
    assert_feature_supported!(
        "SELECT EXTRACT(MINUTE FROM TIME '12:30:45')",
        "F052",
        "EXTRACT MINUTE from TIME"
    );
}

/// F052: EXTRACT SECOND from TIME
#[test]
fn f052_extract_second_from_time() {
    assert_feature_supported!(
        "SELECT EXTRACT(SECOND FROM TIME '12:30:45')",
        "F052",
        "EXTRACT SECOND from TIME"
    );
}

/// F052: EXTRACT from TIMESTAMP
#[test]
fn f052_extract_from_timestamp() {
    assert_feature_supported!(
        "SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-01-15 12:30:45')",
        "F052",
        "EXTRACT YEAR from TIMESTAMP"
    );
}

/// F052: EXTRACT from column
#[test]
fn f052_extract_from_column() {
    assert_feature_supported!(
        "SELECT EXTRACT(YEAR FROM date_col) FROM datetime_types",
        "F052",
        "EXTRACT from column"
    );
}

/// F052: EXTRACT in WHERE clause
#[test]
fn f052_extract_where() {
    assert_feature_supported!(
        "SELECT * FROM datetime_types WHERE EXTRACT(YEAR FROM date_col) = 2024",
        "F052",
        "EXTRACT in WHERE clause"
    );
}

/// F052: EXTRACT DOW (day of week)
#[test]
fn f052_extract_dow() {
    assert_feature_supported!(
        "SELECT EXTRACT(DOW FROM DATE '2024-01-15')",
        "F052",
        "EXTRACT day of week"
    );
}

/// F052: EXTRACT DOY (day of year)
#[test]
fn f052_extract_doy() {
    assert_feature_supported!(
        "SELECT EXTRACT(DOY FROM DATE '2024-01-15')",
        "F052",
        "EXTRACT day of year"
    );
}

/// F052: EXTRACT QUARTER
#[test]
fn f052_extract_quarter() {
    assert_feature_supported!(
        "SELECT EXTRACT(QUARTER FROM DATE '2024-01-15')",
        "F052",
        "EXTRACT quarter"
    );
}

// ============================================================================
// F411: Time zone specification
// ============================================================================

/// F411-01: TIMESTAMP WITH TIME ZONE data type
#[test]
fn f411_01_timestamp_with_timezone_type() {
    assert_feature_supported!(
        "CREATE TABLE t (created_at TIMESTAMP WITH TIME ZONE)",
        "F411-01",
        "TIMESTAMP WITH TIME ZONE data type"
    );
}

/// F411-01: TIMESTAMP WITH TIME ZONE literal
#[test]
fn f411_01_timestamp_with_timezone_literal() {
    assert_feature_supported!(
        "SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 12:30:45+00:00'",
        "F411-01",
        "TIMESTAMP WITH TIME ZONE literal"
    );
}

/// F411-01: TIMESTAMP WITH TIME ZONE with precision
#[test]
fn f411_01_timestamp_with_timezone_precision() {
    assert_feature_supported!(
        "CREATE TABLE t (created_at TIMESTAMP(6) WITH TIME ZONE)",
        "F411-01",
        "TIMESTAMP WITH TIME ZONE with precision"
    );
}

/// F411-02: TIME WITH TIME ZONE data type
#[test]
fn f411_02_time_with_timezone_type() {
    assert_feature_supported!(
        "CREATE TABLE t (start_time TIME WITH TIME ZONE)",
        "F411-02",
        "TIME WITH TIME ZONE data type"
    );
}

/// F411-02: TIME WITH TIME ZONE literal
#[test]
fn f411_02_time_with_timezone_literal() {
    assert_feature_supported!(
        "SELECT TIME WITH TIME ZONE '12:30:45+00:00'",
        "F411-02",
        "TIME WITH TIME ZONE literal"
    );
}

/// F411-03: AT TIME ZONE clause
#[test]
fn f411_03_at_time_zone() {
    assert_feature_supported!(
        "SELECT TIMESTAMP '2024-01-15 12:30:45' AT TIME ZONE 'UTC'",
        "F411-03",
        "AT TIME ZONE clause"
    );
}

/// F411-03: AT TIME ZONE with column
#[test]
fn f411_03_at_time_zone_column() {
    assert_feature_supported!(
        "SELECT timestamp_col AT TIME ZONE 'America/New_York' FROM datetime_types",
        "F411-03",
        "AT TIME ZONE with column"
    );
}

/// F411-03: AT TIME ZONE with offset
#[test]
fn f411_03_at_time_zone_offset() {
    assert_feature_supported!(
        "SELECT TIMESTAMP '2024-01-15 12:30:45' AT TIME ZONE '+05:00'",
        "F411-03",
        "AT TIME ZONE with offset"
    );
}

// ============================================================================
// Additional datetime functions (commonly used)
// ============================================================================

/// CURRENT_TIME function
#[test]
fn current_time_function() {
    assert_feature_supported!(
        "SELECT CURRENT_TIME",
        "F051",
        "CURRENT_TIME function"
    );
}

/// CURRENT_TIMESTAMP function
#[test]
fn current_timestamp_function() {
    assert_feature_supported!(
        "SELECT CURRENT_TIMESTAMP",
        "F051",
        "CURRENT_TIMESTAMP function"
    );
}

/// NOW function (common alias for CURRENT_TIMESTAMP)
#[test]
fn now_function() {
    assert_feature_supported!(
        "SELECT NOW()",
        "F051",
        "NOW function"
    );
}

/// DATE_PART function (synonym for EXTRACT)
#[test]
fn date_part_function() {
    assert_feature_supported!(
        "SELECT DATE_PART('year', DATE '2024-01-15')",
        "F052",
        "DATE_PART function"
    );
}

/// DATE_TRUNC function
#[test]
fn date_trunc_function() {
    assert_feature_supported!(
        "SELECT DATE_TRUNC('month', DATE '2024-01-15')",
        "F052",
        "DATE_TRUNC function"
    );
}

/// DATE_ADD function
#[test]
fn date_add_function() {
    assert_feature_supported!(
        "SELECT DATE_ADD(DATE '2024-01-15', INTERVAL '1' MONTH)",
        "F052",
        "DATE_ADD function"
    );
}

/// DATE_SUB function
#[test]
fn date_sub_function() {
    assert_feature_supported!(
        "SELECT DATE_SUB(DATE '2024-01-15', INTERVAL '1' DAY)",
        "F052",
        "DATE_SUB function"
    );
}

// ============================================================================
// Complex datetime scenarios
// ============================================================================

/// Complex query with multiple datetime operations
#[test]
fn f051_f052_complex_datetime_query() {
    assert_plans!(
        "SELECT
            date_col,
            EXTRACT(YEAR FROM date_col) AS year,
            date_col + INTERVAL '30' DAY AS future_date,
            timestamp_col - INTERVAL '1' HOUR AS past_timestamp,
            CAST(timestamp_col AS DATE) AS timestamp_as_date
         FROM datetime_types
         WHERE date_col BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
           AND EXTRACT(MONTH FROM date_col) IN (1, 6, 12)
         ORDER BY date_col DESC"
    );
}

/// Datetime with aggregation
#[test]
fn f051_f052_datetime_aggregation() {
    assert_plans!(
        "SELECT
            EXTRACT(YEAR FROM date_col) AS year,
            EXTRACT(MONTH FROM date_col) AS month,
            COUNT(*) AS count
         FROM datetime_types
         GROUP BY EXTRACT(YEAR FROM date_col), EXTRACT(MONTH FROM date_col)
         ORDER BY year, month"
    );
}

/// Datetime with joins
#[test]
fn f051_f052_datetime_joins() {
    assert_plans!(
        "SELECT
            p.first_name,
            p.last_name,
            p.birth_date,
            EXTRACT(YEAR FROM p.birth_date) AS birth_year
         FROM person p
         WHERE p.birth_date < CURRENT_DATE - INTERVAL '18' YEAR
         ORDER BY p.birth_date"
    );
}

/// Datetime range queries
#[test]
fn f051_f052_datetime_ranges() {
    assert_plans!(
        "SELECT * FROM datetime_types
         WHERE timestamp_col BETWEEN
               CURRENT_TIMESTAMP - INTERVAL '7' DAY
               AND CURRENT_TIMESTAMP"
    );
}

/// Datetime in subquery
#[test]
fn f051_f052_datetime_subquery() {
    assert_plans!(
        "SELECT * FROM datetime_types
         WHERE date_col IN (
            SELECT date_col FROM datetime_types
            WHERE EXTRACT(YEAR FROM date_col) = 2024
         )"
    );
}

/// Multiple interval additions
#[test]
fn f052_multiple_intervals() {
    assert_plans!(
        "SELECT
            DATE '2024-01-01' + INTERVAL '1' YEAR + INTERVAL '2' MONTH + INTERVAL '3' DAY AS future_date"
    );
}

/// Datetime literal variations
#[test]
fn f051_datetime_literal_variations() {
    assert_plans!(
        "SELECT
            DATE '2024-01-15' AS date_val,
            TIME '12:30:45.123' AS time_val,
            TIMESTAMP '2024-01-15 12:30:45.123456' AS timestamp_val"
    );
}

/// CAST chain between datetime types
#[test]
fn f051_05_cast_chain() {
    assert_plans!(
        "SELECT
            CAST(CAST(TIMESTAMP '2024-01-15 12:30:45' AS DATE) AS VARCHAR) AS date_string,
            CAST(CAST('12:30:45' AS TIME) AS VARCHAR) AS time_string"
    );
}

/// Comparison with multiple datetime types
#[test]
fn f051_04_mixed_datetime_comparison() {
    assert_plans!(
        "SELECT * FROM datetime_types
         WHERE date_col = CAST(timestamp_col AS DATE)
           AND timestamp_col > CAST(DATE '2024-01-01' AS TIMESTAMP)"
    );
}

/// EXTRACT with computed intervals
#[test]
fn f052_extract_computed_interval() {
    assert_plans!(
        "SELECT
            date_col,
            EXTRACT(DAY FROM date_col + INTERVAL '15' DAY) AS computed_day
         FROM datetime_types"
    );
}

// ============================================================================
// Summary test - F051 and F052 combined
// ============================================================================

#[test]
fn f051_f052_summary_comprehensive() {
    // This test verifies that all major F051 and F052 features work together
    // in a realistic scenario combining date/time types, literals, functions,
    // intervals, arithmetic, and extraction

    // Create table with all datetime types
    assert_plans!("CREATE TABLE events (
        event_id INTEGER,
        event_date DATE,
        event_time TIME,
        event_timestamp TIMESTAMP,
        duration INTERVAL DAY,
        timezone_timestamp TIMESTAMP WITH TIME ZONE
    )");

    // Complex query using all datetime features
    assert_plans!(
        "SELECT
            event_date,
            event_time,
            event_timestamp,
            EXTRACT(YEAR FROM event_date) AS year,
            EXTRACT(MONTH FROM event_date) AS month,
            EXTRACT(DAY FROM event_date) AS day,
            EXTRACT(HOUR FROM event_time) AS hour,
            event_date + INTERVAL '7' DAY AS next_week,
            event_timestamp - INTERVAL '1' HOUR AS hour_ago,
            CAST(event_date AS VARCHAR) AS date_string,
            CAST(event_timestamp AS DATE) AS timestamp_date,
            CURRENT_DATE AS today,
            CURRENT_TIMESTAMP AS now
         FROM events
         WHERE event_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
           AND event_timestamp > TIMESTAMP '2024-01-01 00:00:00'
           AND event_time < TIME '18:00:00'
           AND EXTRACT(YEAR FROM event_date) = 2024
         ORDER BY event_timestamp DESC"
    );
}
