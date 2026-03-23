# Eval Results Comparison


## add-relationships-tests

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.2574 | $0.1928 |
| Total Tokens | 3,390 | 1,154 |
| Duration | 68.7s | 28.3s |
| Tools Used | Read, Edit, Bash | Read, Edit, Bash |
| has_customer_relationship | PASS | PASS |
| has_location_relationship | PASS | PASS |
| preserves_expression_test | PASS | PASS |
| preserves_order_id_tests | PASS | PASS |
| used_targeted_dbt_test | PASS | PASS |
| yml_modified | PASS | PASS |

## add-source-freshness

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.1186 | $0.1185 |
| Total Tokens | 703 | 698 |
| Duration | 17.2s | 17.4s |
| Tools Used | Read, Edit, Bash | Read, Edit, Bash |
| error_after_24h | PASS | PASS |
| has_raw_orders_freshness | PASS | PASS |
| preserves_loaded_at_field | PASS | PASS |
| used_targeted_source_freshness | PASS | PASS |
| warn_after_12h | PASS | PASS |
| yml_modified | PASS | PASS |

## add-tests-to-model

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.3550 | $0.3743 |
| Total Tokens | 2,592 | 2,608 |
| Duration | 72.9s | 57.4s |
| Tools Used | Bash, Read, Glob, mcp__claude_ai_MotherDuck__query, Edit | Glob, Read, Bash, Edit |
| has_expression_test | PASS | PASS |
| has_not_null_test | PASS | PASS |
| has_unique_test | PASS | PASS |
| no_accepted_values_hallucination | PASS | PASS |
| used_dbt_show | PASS | PASS |
| yml_modified | PASS | PASS |

## add-unit-test-to-model

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.1898 | $0.1958 |
| Total Tokens | 1,485 | 1,583 |
| Duration | 26.7s | 31.0s |
| Tools Used | Glob, Read, Edit, Bash | Glob, Read, Edit, Bash |
| asserts_boolean_outputs | PASS | PASS |
| has_unit_test | PASS | PASS |
| preserves_product_id_tests | PASS | PASS |
| used_targeted_dbt_test | PASS | PASS |
| uses_raw_products_source_input | PASS | PASS |
| yml_modified | PASS | PASS |

## create-staging-model

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.3991 | $0.3481 |
| Total Tokens | 3,758 | 3,596 |
| Duration | 69.2s | 62.3s |
| Tools Used | Glob, Read, Bash, mcp__claude_ai_MotherDuck__query, Write | Glob, Read, Write, mcp__claude_ai_MotherDuck__list_databases, Bash |
| has_not_null_test | PASS | PASS |
| has_unique_test | PASS | PASS |
| sql_file_created | PASS | PASS |
| uses_cents_to_dollars | PASS | PASS |
| uses_source_macro | PASS | PASS |
| uses_surrogate_key | PASS | PASS |
| yaml_file_created | PASS | PASS |

## debug-failing-build

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.1790 | $0.1635 |
| Total Tokens | 990 | 877 |
| Duration | 28.4s | 31.4s |
| Tools Used | Bash, Read, Edit | Bash, Read, Edit |
| bug_fixed | PASS | PASS |
| no_collateral_changes | PASS | PASS |
| sql_file_modified | PASS | PASS |
| typo_removed | PASS | PASS |

## refactor-hardcoded-source

| Metric | no-skills | with-skills |
|--------|-----|-----|
| Cost | $0.1295 | $0.2867 |
| Total Tokens | 766 | 1,048 |
| Duration | 20.4s | 29.9s |
| Tools Used | Read, Edit, Bash | Read, Glob, Edit, Bash |
| preserves_cents_to_dollars | PASS | PASS |
| preserves_drink_flag | PASS | PASS |
| preserves_food_flag | PASS | PASS |
| removes_hardcoded_relation | PASS | PASS |
| sql_file_modified | PASS | PASS |
| used_targeted_compile | PASS | PASS |
| uses_source_macro | PASS | PASS |