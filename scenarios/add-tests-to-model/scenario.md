# Add Tests to Model

## Description

The `stg_orders` model has column descriptions in its YAML schema file but no data tests. The agent must add appropriate tests based on the model's SQL and actual data.

This scenario specifically tests whether the agent previews data before writing tests (to avoid hallucinating accepted_values) and whether it adds standard tests (not_null, unique) on the primary key.

## Grading Criteria

### Task Completion
1. Added tests to `stg_orders.yml`
2. `dbt test --select stg_orders` passes

### Solution Quality
1. Added `not_null` test on `order_id`
2. Added `unique` test on `order_id`
3. Did NOT hallucinate `accepted_values` test values that don't exist in the data
4. Added `expression_is_true` test for `order_total - tax_paid = subtotal`
5. Bonus: added `not_null` on other important columns (customer_id, location_id)
6. Bonus: added `relationships` test to `stg_customers`

### Tool Usage
- Critical: Used `dbt show --select stg_orders --limit 10` or equivalent to preview data before writing tests
- Appropriate: Read the model SQL to understand column types and relationships
- Inappropriate: Writing accepted_values tests without checking actual data
