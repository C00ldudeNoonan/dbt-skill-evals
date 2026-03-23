# Add Source Freshness

## Description

The project already declares the `raw_orders` source and its `loaded_at_field`, but it does not define freshness expectations yet.

The agent must add freshness configuration for `raw_orders` in `__sources.yml` without disturbing the other source definitions.

## Grading Criteria

### Task Completion
1. Updated `models/staging/__sources.yml`
2. `dbt source freshness --select source:ecom.raw_orders` passes

### Solution Quality
1. Adds a `freshness` block for `raw_orders`
2. Uses `warn_after` of `12 hour`
3. Uses `error_after` of `24 hour`
4. Preserves the existing `loaded_at_field: ordered_at`

### Tool Usage
- Appropriate: Inspect the source definition before editing it
- Appropriate: Run `dbt source freshness --select source:ecom.raw_orders` to validate the change
- Inappropriate: Editing unrelated source tables or changing the source names
