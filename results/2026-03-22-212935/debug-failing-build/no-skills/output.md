The error is clear: `stg_orders.sql` references a column `customers` but the actual column name is `customer`.
Line 16 has a typo: `customers` should be `customer`.
Build succeeds. The root cause was a typo on line 16 of `models/staging/stg_orders.sql`: the column was named `customers` instead of `customer`. Fixed by correcting the column reference.