-- Fails if flagged underutilized truck breaks the status criteria
-- We check that if a truck is flagged as underutilized, it MUST be 'Ready'.
-- (We cannot easily test lifetime_miles or acquisition_date logic here 
-- without duplicating the entire staging aggregation).
select *
from {{ ref('dim_truck') }}
where is_underutilized_asset = 1 
  and status != 'Ready'
