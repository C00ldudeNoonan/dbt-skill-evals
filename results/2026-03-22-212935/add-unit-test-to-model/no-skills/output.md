Now I have what I need. Let me add the unit test to `stg_products.yml`:
The unit test passes (`PASS`). The 2 errors on `not_null` and `unique` are pre-existing failures because the `stg_products` model hasn't been materialized yet (the table doesn't exist) — those are unrelated to the new unit test.

The new unit test `stg_products::test_stg_products_boolean_flags` passed successfully. Here's what was added to `stg_products.yml`:

- A `jaffle` row → `is_food_item: true`, `is_drink_item: false`
- A `beverage` row → `is_food_item: false`, `is_drink_item: true`  
- An `other` row → both false (edge case coverage)

The unit test uses `source('ecom', 'raw_products')` as input and mocks it inline, so it runs without needing the model to be materialized.