The models haven't been built yet. I need to run them first, then test.
All 6 tests pass. The two new relationship tests were added to `stg_orders.yml`:

- `relationships_stg_orders_customer_id__customer_id__ref_stg_customers_` — PASS
- `relationships_stg_orders_location_id__location_id__ref_stg_locations_` — PASS

All pre-existing tests (`not_null`, `unique`, `expression_is_true`) continue to pass as well.