# Create Staging Model

## Description

The jaffle-shop project has a `raw_supplies` source defined but no staging model for it. Other models (e.g., `order_items`) reference `stg_supplies`, so the project won't compile without it.

The agent must create `stg_supplies.sql` and a companion YAML schema file following the conventions of the existing staging models.

## Grading Criteria

### Task Completion
1. Created `stg_supplies.sql` in `models/staging/`
2. Created a YAML schema file (e.g., `stg_supplies.yml`) in `models/staging/`
3. `dbt compile` passes after changes

### Solution Quality
1. Uses `{{ source('ecom', 'raw_supplies') }}` macro (not hardcoded table reference)
2. Follows naming conventions: `stg_supplies` model name, descriptive column aliases
3. YAML file includes `not_null` and `unique` tests on the primary key
4. Materialized as view (inherits from `dbt_project.yml` staging config)
5. Bonus: uses `cents_to_dollars` macro for cost column
6. Bonus: uses `dbt_utils.generate_surrogate_key` for composite key

### Tool Usage
- Appropriate: Read existing models to understand conventions before writing
- Appropriate: Run `dbt compile` to validate
- Inappropriate: Writing without reviewing existing patterns first
