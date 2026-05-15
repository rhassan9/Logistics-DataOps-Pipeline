-- Fails if a driver appears more than once in the same month
select 
    month_sk, 
    driver_sk, 
    count(*) as duplicate_count
from {{ ref('fact_driver_monthly_snapshot') }}
group by month_sk, driver_sk
having count(*) > 1
