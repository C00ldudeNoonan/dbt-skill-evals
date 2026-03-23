# Refactor Hardcoded Source

## Description

The `stg_products` model currently references the raw products table directly instead of using the `source()` macro pattern used throughout the rest of the project.

The agent must refactor the model to use the declared source while preserving the current transformation logic.

## Grading Criteria

### Task Completion
1. Updated `stg_products.sql`
2. `dbt compile --select stg_products` passes

### Solution Quality
1. Uses `{{ source('ecom', 'raw_products') }}` in the source CTE
2. Removes the hardcoded raw table reference
3. Preserves `cents_to_dollars` on `price`
4. Preserves the `is_food_item` and `is_drink_item` boolean derivations

### Tool Usage
- Appropriate: Review nearby staging models before refactoring
- Appropriate: Run `dbt compile --select stg_products` to validate the targeted change
- Inappropriate: Rewriting unrelated logic or changing model grain
