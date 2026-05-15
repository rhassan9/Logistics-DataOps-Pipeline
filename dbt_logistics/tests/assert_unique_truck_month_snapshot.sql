-- Fails if a truck appears more than once in the same month
select 
    month_sk, 
    truck_sk, 
    count(*) as duplicate_count
from {{ ref('fact_truck_monthly_snapshot') }}
group by month_sk, truck_sk
having count(*) > 1
