-- Simple aggregation model — dbt transforms this SQL into a table.
-- This is what dbt does: take raw data, produce a clean summary.
-- Arrowjet handles the bulk export of the result after dbt run completes.

select
    date_trunc('day', sale_date)  as sale_day,
    count(*)                      as total_sales,
    sum(amount)                   as total_amount,
    avg(amount)                   as avg_amount
from {{ source('raw', 'benchmark_test_1m') }}
group by 1
order by 1
