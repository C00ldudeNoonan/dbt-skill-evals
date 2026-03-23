The configuration is valid. The `ERROR STALE` result is expected — dbt successfully parsed the freshness config and evaluated it, but the source data is older than 24 hours (the data is static seed data, not live). The command exiting with an error code is the normal behavior when data is stale; it confirms that:

1. dbt recognized the `freshness` block
2. It queried `ordered_at` from `raw_orders`
3. It applied the thresholds and correctly reported the data as stale

The YAML configuration itself is correct.