-- Fails if fuel swipe gallons is zero or negative
select *
from {{ ref('fact_fuel_purchase') }}
where coalesce(gallons, 0) <= 0
