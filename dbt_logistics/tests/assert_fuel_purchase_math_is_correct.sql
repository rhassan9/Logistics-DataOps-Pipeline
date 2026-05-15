-- Fails if gallons * price_per_gallon != total_cost
select *
from {{ ref('fact_fuel_purchase') }}
where abs(total_cost - (gallons * price_per_gallon)) > 0.05
