Now I have all the context needed. The model uses `type = 'jaffle'` for `is_food_item` and `type = 'beverage'` for `is_drink_item`. I need to add a unit test with `source('ecom', 'raw_products')` as input.
The new unit test `test_stg_products_boolean_flags` **PASS**ed. The two pre-existing `not_null` and `unique` column tests errored because the `stg_products` model hasn't been materialized yet (the table doesn't exist in the DB) — that's a pre-existing condition unrelated to the unit test addition.

The unit test correctly:
- Passes a `jaffle` row → `is_food_item: true`, `is_drink_item: false`
- Passes a `beverage` row → `is_food_item: false`, `is_drink_item: true`
- Passes an `other` row → both flags `false`