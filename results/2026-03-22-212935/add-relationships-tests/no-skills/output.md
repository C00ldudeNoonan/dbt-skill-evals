The models need to be built first. Let me run them, then test.
All 5 tests defined in `stg_orders.yml` pass:
- `not_null_stg_orders_order_id` - PASS
- `unique_stg_orders_order_id` - PASS
- `dbt_utils_expression_is_true` (arithmetic) - PASS
- `relationships_stg_orders_customer_id__customer_id__ref_stg_customers_` - PASS
- `relationships_stg_orders_location_id__location_id__ref_stg_locations_` - PASS

The 1 error (`relationships_stg_order_items_order_id__order_id__ref_stg_orders_`) is a pre-existing failure from `stg_order_items.yml` — dbt's path selector pulls in relationship tests that reference `stg_orders` as the parent, and `stg_order_items` was never built. That's unrelated to the changes made here.