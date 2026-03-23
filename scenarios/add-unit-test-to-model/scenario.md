# Add Unit Test to Model

## Description

The `stg_products` model contains boolean transformation logic based on `product_type`, but that behavior is not covered by a unit test.

The agent must add a unit test to `stg_products.yml` that proves the boolean outputs are computed correctly from representative `raw_products` input rows.

## Grading Criteria

### Task Completion
1. Added a unit test to `stg_products.yml`
2. `dbt test --select stg_products` passes

### Solution Quality
1. The unit test targets `stg_products`
2. The test uses `source('ecom', 'raw_products')` as input
3. The expected rows assert both `is_food_item` and `is_drink_item`
4. The existing `not_null` and `unique` tests on `product_id` are preserved

### Tool Usage
- Appropriate: Read `stg_products.sql` before writing the unit test
- Appropriate: Run `dbt test --select stg_products` to validate the file
- Inappropriate: Replacing the existing column tests instead of adding a unit test
