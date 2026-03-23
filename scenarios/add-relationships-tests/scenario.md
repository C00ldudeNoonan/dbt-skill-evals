# Add Relationship Tests

## Description

The `stg_orders` model already has primary-key and arithmetic coverage, but it is missing foreign-key relationship tests for the parent staging dimensions it references.

The agent must inspect the model SQL and the upstream staging models, then add the missing `relationships` tests to `stg_orders.yml` without disturbing the tests that are already present.

## Grading Criteria

### Task Completion
1. Added relationship tests to `stg_orders.yml`
2. `dbt test --select path:models/staging/stg_orders.yml` passes

### Solution Quality
1. Added a `relationships` test on `customer_id` pointing to `ref('stg_customers')` and `field: customer_id`
2. Added a `relationships` test on `location_id` pointing to `ref('stg_locations')` and `field: location_id`
3. Preserved the existing `not_null` and `unique` tests on `order_id`
4. Preserved the existing `dbt_utils.expression_is_true` test for `order_total - tax_paid = subtotal`

### Tool Usage
- Appropriate: Read `stg_orders.sql` and the parent staging models before writing tests
- Appropriate: Run `dbt test --select path:models/staging/stg_orders.yml` to validate the targeted changes
- Inappropriate: Adding relationships to the wrong parent model or changing unrelated tests
