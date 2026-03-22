# Debug Failing Build

## Description

The jaffle-shop project has a deliberate bug: in `stg_orders.sql`, the column `customer` is misspelled as `customers` (which doesn't exist in `raw_orders`). This causes `dbt build` to fail.

The agent must diagnose the root cause, apply a targeted fix, and verify the build succeeds.

## Grading Criteria

### Task Completion
1. Identified the error is in `stg_orders.sql`
2. Fixed the column name from `customers` to `customer`
3. `dbt build` passes after the fix

### Solution Quality
1. Targeted fix: only changed the necessary line in `stg_orders.sql`
2. Did not modify any other SQL files
3. Did not introduce new issues or unnecessary changes
4. Correctly identified root cause (column name typo, not a schema issue)

### Tool Usage
- Appropriate: Run `dbt build` or `dbt compile` to see the error
- Appropriate: Read the error message and trace to the source file
- Appropriate: Read the source data schema to confirm correct column name
- Inappropriate: Shotgun approach (modifying multiple files without understanding the issue)
