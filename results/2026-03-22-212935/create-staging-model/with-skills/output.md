Now let me check the macros directory and look at one more staging model for reference.
Now I have all I need. From `order_items.sql`, `stg_supplies` needs `product_id` and `supply_cost`. The source is `raw_supplies` with columns `id`, `sku`, and `cost`. Let me create the SQL and YAML files.
Wait — I'm making assumptions about columns beyond what the task specifies. Let me check the actual source table structure first.
The CSV confirms columns: `id`, `name`, `cost`, `perishable`, `sku`. My SQL model is correct. Now create the YAML:
Compiled successfully. Here's what was created:

**`models/staging/stg_supplies.sql`** — follows the `source` → `renamed` CTE pattern used by all other staging models:
- Surrogate key via `dbt_utils.generate_surrogate_key(['id', 'sku'])` → `supply_id`
- `sku` → `product_id` (matches what `order_items.sql` joins on)
- `cost` → `supply_cost` via `cents_to_dollars('cost')` (matches what `order_items.sql` selects)
- `perishable` → `is_perishable_supply`

**`models/staging/stg_supplies.yml`** — `not_null` + `unique` tests on `supply_id`.