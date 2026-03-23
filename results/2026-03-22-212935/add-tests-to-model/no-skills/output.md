Good. The data confirms `subtotal + tax_paid = order_total`. Let me check the available packages for `expression_is_true`.
`dbt_utils.expression_is_true` is available. Now I'll update the YAML.
All 3 new tests pass. The only error (`relationships_stg_order_items_...`) is a pre-existing failure from `stg_order_items.yml` — that model isn't built yet and is unrelated to `stg_orders`.

Here's what was added to `stg_orders.yml`:

```yaml
tests:
  - dbt_utils.expression_is_true:
      expression: "order_total - tax_paid = subtotal"
columns:
  - name: order_id
    tests:
      - unique
      - not_null
```

Results:
- `unique_stg_orders_order_id` — PASS
- `not_null_stg_orders_order_id` — PASS  
- `dbt_utils_expression_is_true_stg_orders_order_total_tax_paid_subtotal` — PASS