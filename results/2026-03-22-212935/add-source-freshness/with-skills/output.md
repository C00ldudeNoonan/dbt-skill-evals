The configuration is valid. The `ERROR STALE` result is expected — it means dbt successfully evaluated the freshness thresholds and determined the data is stale (older than 24 hours), which is correct behavior for a static test dataset. The configuration itself parsed and executed without errors.

The `freshness` block has been added to `raw_orders` in `models/staging/__sources.yml`:
- `warn_after: {count: 12, period: hour}`
- `error_after: {count: 24, period: hour}`