Now let me preview the actual data:
The `dbt show` fails due to an unrelated test on `stg_order_items`. Let me query the data directly:
Data confirmed: `order_id` is unique and never null (61,948 rows, 61,948 distinct), and `order_total - tax_paid = subtotal` holds for all rows. Now I'll add the tests:
All 3 new tests pass. The one error (`relationships_stg_order_items_order_id...`) is a pre-existing issue from `stg_order_items.yml` — `stg_order_items` hasn't been built yet, and that test was failing before my changes.

Here's what was added to `stg_orders.yml`:

```yaml
models:
  - name: stg_orders
    ...
    tests:
      - dbt_utils.expression_is_true:
          expression: "order_total - tax_paid = subtotal"
    columns:
      - name: order_id
        ...
        tests:
          - unique
          - not_null
```

Results:
- `unique_stg_orders_order_id` — PASS (61,948 distinct UUIDs)
- `not_null_stg_orders_order_id` — PASS (0 nulls)
- `dbt_utils_expression_is_true_stg_orders_order_total_tax_paid_subtotal` — PASS (0 failing rows)