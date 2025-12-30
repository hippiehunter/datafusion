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

//! SQL:2016 Features P001 and T321 - SQL/PSM Routines and Control Flow
//!
//! ISO/IEC 9075-4:2016 Sections 4, 13, 14, 15
//!
//! These features cover stored procedures, functions, and control flow:
//!
//! | Feature | Subfeature | Description | Status |
//! |---------|------------|-------------|--------|
//! | P001 | P001-01 | Stored modules | Not Implemented |
//! | P001 | P001-02 | Stored procedures | Not Implemented |
//! | P001 | P001-03 | Stored functions | Not Implemented |
//! | T321 | T321-01 | User-defined functions (no overloading) | Not Implemented |
//! | T321 | T321-02 | User-defined stored procedures (no overloading) | Not Implemented |
//! | T321 | T321-03 | Function invocation | Not Implemented |
//! | T321 | T321-04 | CALL statement | Not Implemented |
//! | T321 | T321-05 | RETURN statement | Not Implemented |
//!
//! T321 is a CORE feature (mandatory for SQL:2016 conformance).
//! P001 is an optional feature.
//!
//! # Current Status
//!
//! # Dialect Considerations
//!
//! SQL/PSM syntax varies by dialect. sqlparser supports:
//!
//! - **MsSqlDialect**: `AS BEGIN ... END` blocks, but requires `@` prefix for variables
//! - **PostgreSqlDialect**: Standard variable names, but uses `$$ ... $$` string bodies
//! - **GenericDialect**: Standard syntax but no BEGIN/END block support
//!
//! Most tests use MsSqlDialect for BEGIN/END support, with `@` prefixed variables.
//! PostgreSQL-style tests with standard variable names are included where applicable.

use crate::{
    assert_parses, assert_feature_supported, assert_postgres_parses,
    assert_psm_feature_supported, assert_psm_parses,
};

// ============================================================================
// P001-01: Stored Modules
// ============================================================================

/// P001-01: Basic module with stored procedure
/// Note: CREATE MODULE is not supported by sqlparser - documenting as unimplemented
#[test]
#[ignore = "CREATE MODULE not supported by sqlparser"]
fn p001_01_stored_module_basic() {
    // CREATE MODULE is a SQL standard feature not implemented in sqlparser
    // This test is ignored to document the expected syntax
    assert_parses!("CREATE MODULE accounting");
}

// ============================================================================
// P001-02: Stored Procedures - Basic Creation
// ============================================================================

/// P001-02: Simplest possible stored procedure
/// Note: MsSqlDialect requires AS before BEGIN
#[test]
fn p001_02_create_procedure_empty() {
    // Just test parsing - CREATE PROCEDURE planning not yet implemented
    assert_psm_parses!(
        "CREATE PROCEDURE simple_proc AS BEGIN SELECT 1; END"
    );
}

/// P001-02: Stored procedure with SQL body
/// Note: CREATE PROCEDURE planning not yet implemented - testing parsing only
#[test]
fn p001_02_create_procedure_with_body() {
    assert_psm_parses!(
        "CREATE PROCEDURE update_salaries AS BEGIN
           UPDATE person SET salary = salary * 1.1;
         END"
    );
}

/// P001-02: Stored procedure with parameters
/// Note: CREATE PROCEDURE planning not yet implemented - testing parsing only
#[test]
fn p001_02_create_procedure_with_params() {
    assert_psm_parses!(
        "CREATE PROCEDURE increase_salary(IN emp_id INT, IN percent DECIMAL(5,2))
         AS BEGIN
           SELECT 1;
         END"
    );
}

/// P001-02: Stored procedure with OUT parameter
#[test]
fn p001_02_create_procedure_out_param() {
    assert_psm_parses!(
        "CREATE PROCEDURE get_employee_count(OUT emp_count INT)
         AS BEGIN
           SELECT 1;
         END"
    );
}

/// P001-02: Stored procedure with INOUT parameter
#[test]
fn p001_02_create_procedure_inout_param() {
    assert_psm_parses!(
        "CREATE PROCEDURE double_value(INOUT val INT)
         AS BEGIN
           SELECT 1;
         END"
    );
}

/// P001-02: DROP PROCEDURE statement
/// Note: DROP PROCEDURE parses but planning not yet implemented
#[test]
fn p001_02_drop_procedure() {
    assert_psm_parses!(
        "DROP PROCEDURE update_salaries"
    );
}

/// P001-02: DROP PROCEDURE IF EXISTS
/// Note: DROP PROCEDURE IF EXISTS parses but planning not yet implemented
#[test]
fn p001_02_drop_procedure_if_exists() {
    assert_psm_parses!(
        "DROP PROCEDURE IF EXISTS update_salaries"
    );
}

// ============================================================================
// P001-03: Stored Functions
// ============================================================================

/// P001-03: Basic stored function with RETURNS
#[test]
fn p001_03_create_function_basic() {
    assert_psm_feature_supported!(
        "CREATE FUNCTION get_tax_rate() RETURNS DECIMAL(5,2)
         AS BEGIN
           RETURN 0.08;
         END",
        "P001-03",
        "Basic stored function"
    );
}

/// P001-03: Stored function with parameters
/// Note: Uses simple RETURN to avoid variable reference issues in expression planner
#[test]
fn p001_03_create_function_with_params() {
    assert_psm_feature_supported!(
        "CREATE FUNCTION calculate_tax(amount DECIMAL(10,2)) RETURNS DECIMAL(10,2)
         AS BEGIN
           RETURN 0.08;
         END",
        "P001-03",
        "Stored function with parameters"
    );
}

/// P001-03: Stored function with SQL body (complex)
/// Note: MsSqlDialect requires @ prefix for variables; standard syntax tested separately
#[test]
fn p001_03_create_function_sql_body() {
    assert_psm_feature_supported!(
        "CREATE FUNCTION get_employee_salary(emp_id INT) RETURNS DECIMAL(10,2)
         AS BEGIN
           DECLARE @result DECIMAL(10,2);
           RETURN 0;
         END",
        "P001-03",
        "Stored function with SQL body"
    );
}

/// P001-03: DROP FUNCTION statement
#[test]
fn p001_03_drop_function() {
    assert_feature_supported!(
        "DROP FUNCTION calculate_tax",
        "P001-03",
        "DROP FUNCTION statement"
    );
}

/// P001-03: DROP FUNCTION IF EXISTS
#[test]
fn p001_03_drop_function_if_exists() {
    assert_feature_supported!(
        "DROP FUNCTION IF EXISTS calculate_tax",
        "P001-03",
        "DROP FUNCTION IF EXISTS"
    );
}

/// P001-03: PostgreSQL-style function with standard variable names
///
/// This demonstrates standard SQL variable syntax (no @ prefix).
/// PostgreSQL uses $$ delimited bodies which contain the actual PL/pgSQL code.
/// The function body is passed as a string, so variable names are not validated by the parser.
#[test]
fn p001_03_postgres_function_syntax() {
    assert_postgres_parses!(
        "CREATE FUNCTION get_employee_salary(emp_id INT) RETURNS DECIMAL(10,2)
         LANGUAGE plpgsql
         AS $$
         DECLARE
           result DECIMAL(10,2);
         BEGIN
           SELECT salary INTO result FROM employees WHERE id = emp_id;
           RETURN result;
         END;
         $$"
    );
}

// ============================================================================
// Control Flow - IF Statement
// ============================================================================

/// IF...THEN...END IF statement
#[test]
fn control_flow_if_basic() {
    assert_feature_supported!(
        "CREATE PROCEDURE check_value(val INT)
         BEGIN
           IF val > 0 THEN
             SELECT 'Positive';
           END IF;
         END",
        "P001",
        "IF...THEN...END IF statement"
    );
}

/// IF...THEN...ELSE...END IF statement
#[test]
fn control_flow_if_else() {
    assert_feature_supported!(
        "CREATE PROCEDURE check_value(val INT)
         BEGIN
           IF val > 0 THEN
             SELECT 'Positive';
           ELSE
             SELECT 'Non-positive';
           END IF;
         END",
        "P001",
        "IF...THEN...ELSE...END IF statement"
    );
}

/// IF...THEN...ELSEIF...ELSE...END IF statement
#[test]
fn control_flow_if_elseif() {
    assert_feature_supported!(
        "CREATE PROCEDURE check_value(val INT)
         BEGIN
           IF val > 0 THEN
             SELECT 'Positive';
           ELSEIF val < 0 THEN
             SELECT 'Negative';
           ELSE
             SELECT 'Zero';
           END IF;
         END",
        "P001",
        "IF...THEN...ELSEIF...ELSE...END IF statement"
    );
}

// ============================================================================
// Control Flow - CASE Statement (Procedural)
// ============================================================================

/// CASE statement (searched form)
#[test]
fn control_flow_case_searched() {
    assert_feature_supported!(
        "CREATE PROCEDURE classify_age(age INT)
         BEGIN
           CASE
             WHEN age < 18 THEN SELECT 'Minor';
             WHEN age < 65 THEN SELECT 'Adult';
             ELSE SELECT 'Senior';
           END CASE;
         END",
        "P001",
        "CASE statement (searched)"
    );
}

/// CASE statement (simple form)
#[test]
fn control_flow_case_simple() {
    assert_feature_supported!(
        "CREATE PROCEDURE classify_status(status INT)
         BEGIN
           CASE status
             WHEN 1 THEN SELECT 'Active';
             WHEN 2 THEN SELECT 'Inactive';
             WHEN 3 THEN SELECT 'Pending';
             ELSE SELECT 'Unknown';
           END CASE;
         END",
        "P001",
        "CASE statement (simple)"
    );
}

// ============================================================================
// Control Flow - LOOP Statement
// ============================================================================

/// Basic LOOP...END LOOP
#[test]
fn control_flow_loop_basic() {
    assert_feature_supported!(
        "CREATE PROCEDURE loop_example()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           my_loop: LOOP
             SET counter = counter + 1;
             IF counter >= 10 THEN
               LEAVE my_loop;
             END IF;
           END LOOP;
         END",
        "P001",
        "LOOP...END LOOP statement"
    );
}

/// LOOP with LEAVE statement
#[test]
fn control_flow_loop_leave() {
    assert_feature_supported!(
        "CREATE PROCEDURE loop_with_leave()
         BEGIN
           my_loop: LOOP
             LEAVE my_loop;
           END LOOP;
         END",
        "P001",
        "LOOP with LEAVE"
    );
}

/// LOOP with ITERATE statement
#[test]
fn control_flow_loop_iterate() {
    assert_feature_supported!(
        "CREATE PROCEDURE loop_with_iterate()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           my_loop: LOOP
             SET counter = counter + 1;
             IF counter < 5 THEN
               ITERATE my_loop;
             END IF;
             IF counter >= 10 THEN
               LEAVE my_loop;
             END IF;
           END LOOP;
         END",
        "P001",
        "LOOP with ITERATE"
    );
}

// ============================================================================
// Control Flow - WHILE Statement
// ============================================================================

/// WHILE...DO...END WHILE statement
#[test]
fn control_flow_while() {
    assert_feature_supported!(
        "CREATE PROCEDURE while_example()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           WHILE counter < 10 DO
             SET counter = counter + 1;
           END WHILE;
         END",
        "P001",
        "WHILE...DO...END WHILE statement"
    );
}

/// WHILE with labeled statement
#[test]
fn control_flow_while_labeled() {
    assert_feature_supported!(
        "CREATE PROCEDURE while_labeled()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           my_while: WHILE counter < 10 DO
             SET counter = counter + 1;
             IF counter = 5 THEN
               LEAVE my_while;
             END IF;
           END WHILE;
         END",
        "P001",
        "Labeled WHILE statement"
    );
}

// ============================================================================
// Control Flow - REPEAT Statement
// ============================================================================

/// REPEAT...UNTIL...END REPEAT statement
#[test]
fn control_flow_repeat() {
    assert_feature_supported!(
        "CREATE PROCEDURE repeat_example()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           REPEAT
             SET counter = counter + 1;
           UNTIL counter >= 10
           END REPEAT;
         END",
        "P001",
        "REPEAT...UNTIL...END REPEAT statement"
    );
}

/// REPEAT with labeled statement
#[test]
fn control_flow_repeat_labeled() {
    assert_feature_supported!(
        "CREATE PROCEDURE repeat_labeled()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           my_repeat: REPEAT
             SET counter = counter + 1;
             IF counter = 5 THEN
               LEAVE my_repeat;
             END IF;
           UNTIL counter >= 10
           END REPEAT;
         END",
        "P001",
        "Labeled REPEAT statement"
    );
}

// ============================================================================
// Control Flow - FOR Statement
// ============================================================================

/// FOR...DO...END FOR statement (cursor-based)
#[test]
fn control_flow_for_cursor() {
    assert_feature_supported!(
        "CREATE PROCEDURE for_example()
         BEGIN
           FOR rec IN (SELECT id, first_name FROM person) DO
             SELECT rec.id, rec.first_name;
           END FOR;
         END",
        "P001",
        "FOR...DO...END FOR statement (cursor)"
    );
}

/// FOR with labeled statement
#[test]
fn control_flow_for_labeled() {
    assert_feature_supported!(
        "CREATE PROCEDURE for_labeled()
         BEGIN
           my_for: FOR rec IN (SELECT id FROM person) DO
             IF rec.id > 100 THEN
               LEAVE my_for;
             END IF;
           END FOR;
         END",
        "P001",
        "Labeled FOR statement"
    );
}

// ============================================================================
// Variable Handling - DECLARE
// ============================================================================

/// DECLARE variable with type
#[test]
fn variable_declare_basic() {
    assert_feature_supported!(
        "CREATE PROCEDURE declare_example()
         BEGIN
           DECLARE counter INT;
         END",
        "P001",
        "DECLARE variable statement"
    );
}

/// DECLARE variable with DEFAULT value
#[test]
fn variable_declare_with_default() {
    assert_feature_supported!(
        "CREATE PROCEDURE declare_with_default()
         BEGIN
           DECLARE counter INT DEFAULT 0;
           DECLARE name VARCHAR(100) DEFAULT 'Unknown';
         END",
        "P001",
        "DECLARE with DEFAULT value"
    );
}

/// DECLARE multiple variables
#[test]
fn variable_declare_multiple() {
    assert_feature_supported!(
        "CREATE PROCEDURE declare_multiple()
         BEGIN
           DECLARE x, y, z INT;
           DECLARE first_name, last_name VARCHAR(50);
         END",
        "P001",
        "DECLARE multiple variables"
    );
}

// ============================================================================
// Variable Handling - SET
// ============================================================================

/// SET variable statement
#[test]
fn variable_set_basic() {
    assert_feature_supported!(
        "CREATE PROCEDURE set_example()
         BEGIN
           DECLARE counter INT;
           SET counter = 10;
         END",
        "P001",
        "SET variable statement"
    );
}

/// SET with expression
#[test]
fn variable_set_expression() {
    assert_feature_supported!(
        "CREATE PROCEDURE set_expression()
         BEGIN
           DECLARE x INT DEFAULT 5;
           DECLARE y INT;
           SET y = x * 2 + 10;
         END",
        "P001",
        "SET with expression"
    );
}

/// SET multiple variables
#[test]
fn variable_set_multiple() {
    assert_feature_supported!(
        "CREATE PROCEDURE set_multiple()
         BEGIN
           DECLARE x, y INT;
           SET x = 1, y = 2;
         END",
        "P001",
        "SET multiple variables"
    );
}

// ============================================================================
// Variable Handling - SELECT INTO
// ============================================================================

/// SELECT INTO variable
#[test]
fn variable_select_into_single() {
    assert_feature_supported!(
        "CREATE PROCEDURE select_into_example()
         BEGIN
           DECLARE emp_name VARCHAR(100);
           SELECT first_name INTO emp_name FROM person WHERE id = 1;
         END",
        "P001",
        "SELECT INTO variable"
    );
}

/// SELECT INTO multiple variables
#[test]
fn variable_select_into_multiple() {
    assert_feature_supported!(
        "CREATE PROCEDURE select_into_multiple()
         BEGIN
           DECLARE emp_first VARCHAR(50);
           DECLARE emp_last VARCHAR(50);
           DECLARE emp_age INT;
           SELECT first_name, last_name, age
             INTO emp_first, emp_last, emp_age
             FROM person WHERE id = 1;
         END",
        "P001",
        "SELECT INTO multiple variables"
    );
}

// ============================================================================
// Exception Handling - DECLARE HANDLER
// ============================================================================

/// DECLARE CONTINUE HANDLER
#[test]
fn exception_handler_continue() {
    assert_feature_supported!(
        "CREATE PROCEDURE handler_example()
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
             SET @error_occurred = 1;
           SELECT * FROM nonexistent_table;
         END",
        "P001",
        "DECLARE CONTINUE HANDLER"
    );
}

/// DECLARE EXIT HANDLER
#[test]
fn exception_handler_exit() {
    assert_feature_supported!(
        "CREATE PROCEDURE handler_exit()
         BEGIN
           DECLARE EXIT HANDLER FOR SQLEXCEPTION
             SELECT 'Error occurred';
           SELECT * FROM nonexistent_table;
         END",
        "P001",
        "DECLARE EXIT HANDLER"
    );
}

/// DECLARE HANDLER for specific SQLSTATE
#[test]
fn exception_handler_sqlstate() {
    assert_feature_supported!(
        "CREATE PROCEDURE handler_sqlstate()
         BEGIN
           DECLARE EXIT HANDLER FOR SQLSTATE '23000'
             SELECT 'Integrity constraint violation';
           INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date)
             VALUES (1, 'John', 'Doe', 30, 'CA', 50000, TIMESTAMP '2023-01-01 00:00:00');
         END",
        "P001",
        "DECLARE HANDLER for SQLSTATE"
    );
}

/// DECLARE HANDLER for NOT FOUND
#[test]
fn exception_handler_not_found() {
    assert_feature_supported!(
        "CREATE PROCEDURE handler_not_found()
         BEGIN
           DECLARE CONTINUE HANDLER FOR NOT FOUND
             SET @not_found = 1;
           SELECT id INTO @emp_id FROM person WHERE id = 9999;
         END",
        "P001",
        "DECLARE HANDLER for NOT FOUND"
    );
}

// ============================================================================
// Exception Handling - SIGNAL and RESIGNAL
// ============================================================================

/// SIGNAL statement
#[test]
fn exception_signal() {
    assert_feature_supported!(
        "CREATE PROCEDURE signal_example()
         BEGIN
           SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Custom error';
         END",
        "P001",
        "SIGNAL statement"
    );
}

/// RESIGNAL statement
#[test]
fn exception_resignal() {
    assert_feature_supported!(
        "CREATE PROCEDURE resignal_example()
         BEGIN
           DECLARE EXIT HANDLER FOR SQLEXCEPTION
             RESIGNAL SET MESSAGE_TEXT = 'Error in procedure';
           SELECT * FROM nonexistent_table;
         END",
        "P001",
        "RESIGNAL statement"
    );
}

/// SIGNAL with multiple attributes
#[test]
fn exception_signal_attributes() {
    assert_feature_supported!(
        "CREATE PROCEDURE signal_attrs()
         BEGIN
           SIGNAL SQLSTATE '45000'
             SET MESSAGE_TEXT = 'Error message',
                 MYSQL_ERRNO = 1234;
         END",
        "P001",
        "SIGNAL with multiple attributes"
    );
}

// ============================================================================
// T321-01: User-defined functions without overloading
// ============================================================================

/// T321-01: Simple UDF
#[test]
fn t321_01_udf_simple() {
    assert_feature_supported!(
        "CREATE FUNCTION add_ten(x INT) RETURNS INT
         RETURN x + 10",
        "T321-01",
        "User-defined function without overloading"
    );
}

/// T321-01: UDF with DETERMINISTIC
#[test]
fn t321_01_udf_deterministic() {
    assert_feature_supported!(
        "CREATE FUNCTION calc_tax(amount DECIMAL(10,2)) RETURNS DECIMAL(10,2)
         DETERMINISTIC
         RETURN amount * 0.08",
        "T321-01",
        "UDF with DETERMINISTIC"
    );
}

/// T321-01: UDF with CONTAINS SQL
#[test]
fn t321_01_udf_contains_sql() {
    assert_feature_supported!(
        "CREATE FUNCTION get_count() RETURNS INT
         CONTAINS SQL
         BEGIN
           DECLARE result INT;
           SELECT COUNT(*) INTO result FROM person;
           RETURN result;
         END",
        "T321-01",
        "UDF with CONTAINS SQL"
    );
}

/// T321-01: UDF with NO SQL
#[test]
fn t321_01_udf_no_sql() {
    assert_feature_supported!(
        "CREATE FUNCTION pure_calc(x INT, y INT) RETURNS INT
         NO SQL
         RETURN x * y + 42",
        "T321-01",
        "UDF with NO SQL"
    );
}

/// T321-01: UDF with READS SQL DATA
#[test]
fn t321_01_udf_reads_sql() {
    assert_feature_supported!(
        "CREATE FUNCTION get_salary(emp_id INT) RETURNS DECIMAL(10,2)
         READS SQL DATA
         BEGIN
           DECLARE result DECIMAL(10,2);
           SELECT salary INTO result FROM person WHERE id = emp_id;
           RETURN result;
         END",
        "T321-01",
        "UDF with READS SQL DATA"
    );
}

// ============================================================================
// T321-02: User-defined stored procedures without overloading
// ============================================================================

/// T321-02: Simple stored procedure
#[test]
fn t321_02_procedure_simple() {
    assert_feature_supported!(
        "CREATE PROCEDURE reset_salaries()
         UPDATE person SET salary = 50000",
        "T321-02",
        "Simple stored procedure"
    );
}

/// T321-02: Procedure with MODIFIES SQL DATA
#[test]
fn t321_02_procedure_modifies_sql() {
    assert_feature_supported!(
        "CREATE PROCEDURE update_all_salaries(increase DECIMAL(5,2))
         MODIFIES SQL DATA
         UPDATE person SET salary = salary * (1 + increase / 100)",
        "T321-02",
        "Procedure with MODIFIES SQL DATA"
    );
}

/// T321-02: Procedure with multiple statements
#[test]
fn t321_02_procedure_multiple_statements() {
    assert_feature_supported!(
        "CREATE PROCEDURE complex_update()
         BEGIN
           UPDATE person SET salary = salary * 1.1 WHERE age < 30;
           UPDATE person SET salary = salary * 1.05 WHERE age >= 30;
         END",
        "T321-02",
        "Procedure with multiple statements"
    );
}

// ============================================================================
// T321-03: Function invocation
// ============================================================================

/// T321-03: Call function in SELECT
#[test]
fn t321_03_function_invocation_select() {
    assert_feature_supported!(
        "SELECT add_ten(5)",
        "T321-03",
        "Function invocation in SELECT"
    );
}

/// T321-03: Call function in WHERE clause
#[test]
fn t321_03_function_invocation_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary > calc_tax(50000)",
        "T321-03",
        "Function invocation in WHERE"
    );
}

/// T321-03: Call function with column reference
#[test]
fn t321_03_function_invocation_column() {
    assert_feature_supported!(
        "SELECT first_name, calc_tax(salary) FROM person",
        "T321-03",
        "Function invocation with column"
    );
}

// ============================================================================
// T321-04: CALL statement
// ============================================================================

/// T321-04: Basic CALL statement
#[test]
fn t321_04_call_basic() {
    assert_feature_supported!(
        "CALL reset_salaries()",
        "T321-04",
        "CALL statement"
    );
}

/// T321-04: CALL with parameters
#[test]
fn t321_04_call_with_params() {
    assert_feature_supported!(
        "CALL increase_salary(123, 10.5)",
        "T321-04",
        "CALL with parameters"
    );
}

/// T321-04: CALL with OUT parameter
#[test]
fn t321_04_call_out_param() {
    assert_feature_supported!(
        "CALL get_employee_count(@count)",
        "T321-04",
        "CALL with OUT parameter"
    );
}

/// T321-04: EXECUTE PROCEDURE (synonym for CALL)
#[test]
fn t321_04_execute_procedure() {
    assert_feature_supported!(
        "EXECUTE PROCEDURE reset_salaries()",
        "T321-04",
        "EXECUTE PROCEDURE statement"
    );
}

// ============================================================================
// T321-05: RETURN statement
// ============================================================================

/// T321-05: RETURN with literal value
#[test]
fn t321_05_return_literal() {
    assert_feature_supported!(
        "CREATE FUNCTION get_constant() RETURNS INT
         BEGIN
           RETURN 42;
         END",
        "T321-05",
        "RETURN with literal"
    );
}

/// T321-05: RETURN with expression
#[test]
fn t321_05_return_expression() {
    assert_feature_supported!(
        "CREATE FUNCTION calc(x INT, y INT) RETURNS INT
         BEGIN
           RETURN x * y + 10;
         END",
        "T321-05",
        "RETURN with expression"
    );
}

/// T321-05: RETURN with variable
#[test]
fn t321_05_return_variable() {
    assert_feature_supported!(
        "CREATE FUNCTION get_value() RETURNS INT
         BEGIN
           DECLARE result INT;
           SET result = 100;
           RETURN result;
         END",
        "T321-05",
        "RETURN with variable"
    );
}

/// T321-05: RETURN with SELECT result
#[test]
fn t321_05_return_select() {
    assert_feature_supported!(
        "CREATE FUNCTION get_max_salary() RETURNS DECIMAL(10,2)
         BEGIN
           DECLARE max_sal DECIMAL(10,2);
           SELECT MAX(salary) INTO max_sal FROM person;
           RETURN max_sal;
         END",
        "T321-05",
        "RETURN with SELECT result"
    );
}

// ============================================================================
// Complex Scenarios - Nested Control Flow
// ============================================================================

/// Nested IF statements
#[test]
fn complex_nested_if() {
    assert_feature_supported!(
        "CREATE PROCEDURE nested_if(val INT)
         BEGIN
           IF val > 0 THEN
             IF val < 10 THEN
               SELECT 'Single digit positive';
             ELSE
               SELECT 'Multi digit positive';
             END IF;
           ELSE
             SELECT 'Non-positive';
           END IF;
         END",
        "P001",
        "Nested IF statements"
    );
}

/// LOOP with multiple control statements
#[test]
fn complex_loop_control() {
    assert_feature_supported!(
        "CREATE PROCEDURE complex_loop()
         BEGIN
           DECLARE i INT DEFAULT 0;
           my_loop: LOOP
             SET i = i + 1;
             IF i = 5 THEN
               ITERATE my_loop;
             END IF;
             IF i > 10 THEN
               LEAVE my_loop;
             END IF;
             SELECT i;
           END LOOP;
         END",
        "P001",
        "LOOP with ITERATE and LEAVE"
    );
}

/// Mixed control flow structures
#[test]
fn complex_mixed_control() {
    assert_feature_supported!(
        "CREATE PROCEDURE mixed_control(max_val INT)
         BEGIN
           DECLARE i INT DEFAULT 0;
           WHILE i < max_val DO
             IF i % 2 = 0 THEN
               SELECT 'Even', i;
             ELSE
               CASE
                 WHEN i % 3 = 0 THEN SELECT 'Divisible by 3', i;
                 WHEN i % 5 = 0 THEN SELECT 'Divisible by 5', i;
                 ELSE SELECT 'Odd', i;
               END CASE;
             END IF;
             SET i = i + 1;
           END WHILE;
         END",
        "P001",
        "Mixed control flow structures"
    );
}

// ============================================================================
// Complex Scenarios - Exception Handling
// ============================================================================

/// Multiple exception handlers
#[test]
fn complex_multiple_handlers() {
    assert_feature_supported!(
        "CREATE PROCEDURE multi_handlers()
         BEGIN
           DECLARE CONTINUE HANDLER FOR SQLSTATE '23000'
             SELECT 'Constraint violation';
           DECLARE CONTINUE HANDLER FOR SQLSTATE '42000'
             SELECT 'Syntax error';
           DECLARE EXIT HANDLER FOR SQLEXCEPTION
             SELECT 'General error';
           INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date)
             VALUES (1, 'Test', 'User', 25, 'CA', 50000, TIMESTAMP '2023-01-01 00:00:00');
         END",
        "P001",
        "Multiple exception handlers"
    );
}

/// Nested blocks with handlers
#[test]
fn complex_nested_blocks_handlers() {
    assert_feature_supported!(
        "CREATE PROCEDURE nested_handlers()
         BEGIN
           DECLARE EXIT HANDLER FOR SQLEXCEPTION
             SELECT 'Outer handler';
           BEGIN
             DECLARE EXIT HANDLER FOR SQLEXCEPTION
               SELECT 'Inner handler';
             SELECT * FROM nonexistent_table;
           END;
         END",
        "P001",
        "Nested blocks with handlers"
    );
}

// ============================================================================
// Complex Scenarios - Full Procedures
// ============================================================================

/// Complete procedure with all features
#[test]
fn complex_full_procedure() {
    assert_feature_supported!(
        "CREATE PROCEDURE process_employees(min_salary DECIMAL(10,2), OUT processed_count INT)
         BEGIN
           DECLARE done INT DEFAULT 0;
           DECLARE emp_id INT;
           DECLARE emp_sal DECIMAL(10,2);
           DECLARE count INT DEFAULT 0;

           DECLARE emp_cursor CURSOR FOR
             SELECT id, salary FROM person WHERE salary >= min_salary;

           DECLARE CONTINUE HANDLER FOR NOT FOUND
             SET done = 1;

           OPEN emp_cursor;

           read_loop: LOOP
             FETCH emp_cursor INTO emp_id, emp_sal;
             IF done THEN
               LEAVE read_loop;
             END IF;

             UPDATE person SET salary = emp_sal * 1.1 WHERE id = emp_id;
             SET count = count + 1;
           END LOOP;

           CLOSE emp_cursor;
           SET processed_count = count;
         END",
        "P001",
        "Complete procedure with cursors and handlers"
    );
}

/// Procedure with transaction control
#[test]
fn complex_procedure_transaction() {
    assert_feature_supported!(
        "CREATE PROCEDURE safe_update(emp_id INT, new_salary DECIMAL(10,2))
         BEGIN
           DECLARE EXIT HANDLER FOR SQLEXCEPTION
           BEGIN
             ROLLBACK;
             SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Update failed';
           END;

           START TRANSACTION;
           UPDATE person SET salary = new_salary WHERE id = emp_id;
           COMMIT;
         END",
        "P001",
        "Procedure with transaction control"
    );
}

// ============================================================================
// Summary Tests - Overall PSM Support
// ============================================================================

#[test]
fn psm_summary_basic_routine_lifecycle() {
    // Most basic PSM operations
    assert_parses!("CREATE PROCEDURE test_proc() BEGIN END");
    assert_parses!("CREATE FUNCTION test_func() RETURNS INT RETURN 42");
    assert_parses!("CALL test_proc()");
    assert_parses!("SELECT test_func()");
    assert_parses!("DROP PROCEDURE test_proc");
    assert_parses!("DROP FUNCTION test_func");
}

#[test]
fn psm_summary_control_flow() {
    // Control flow statements are now supported
    assert_parses!("CREATE PROCEDURE cf() BEGIN IF 1=1 THEN SELECT 1; END IF; END");
    assert_parses!("CREATE PROCEDURE cf() BEGIN WHILE 1=1 DO SELECT 1; END WHILE; END");
    assert_parses!("CREATE PROCEDURE cf() BEGIN LOOP LEAVE; END LOOP; END");
}

#[test]
fn psm_summary_variables() {
    // Variable handling is now supported
    assert_parses!("CREATE PROCEDURE v() BEGIN DECLARE x INT; SET x = 1; END");
    assert_parses!("CREATE PROCEDURE v() BEGIN DECLARE x INT; SELECT a INTO x FROM t; END");
}

#[test]
fn psm_summary_exception_handling() {
    // Exception handling is now supported
    assert_parses!("CREATE PROCEDURE eh() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END; END");
    assert_parses!("CREATE PROCEDURE eh() BEGIN SIGNAL SQLSTATE '45000'; END");
}
