Let me also check the project config and see if I can query the source table schema.
Let me check the profiles to understand the DB connection and query the source table.
Now I have everything I need. The source has columns: `id`, `name`, `cost`, `perishable`, `sku`. Let me create the model and YAML.
Compiled successfully. The model resolves correctly with:
- `dbt_utils.generate_surrogate_key(['id', 'sku'])` → MD5 hash of both columns
- `cents_to_dollars('cost')` → `(cost / 100)::numeric(16, 2)`

**Summary of what was created:**

**`models/staging/stg_supplies.sql`** — follows the `source` → `renamed` CTE pattern with:
- `supply_id` — surrogate key via `dbt_utils.generate_surrogate_key(['id', 'sku'])`, handles the composite grain
- `supply_id_raw` — the raw `id` for reference
- `product_id` — renamed from `sku`, matching what `order_items.sql` joins on
- `supply_name` — renamed from `name`
- `supply_cost` — renamed from `cost` via `{{ cents_to_dollars('cost') }}`
- `is_perishable` — renamed from `perishable`

**`models/staging/stg_supplies.yml`** — documents all columns with `not_null` + `unique` tests on `supply_id`.