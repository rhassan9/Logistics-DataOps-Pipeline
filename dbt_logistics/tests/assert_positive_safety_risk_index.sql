-- Fails if driver_safety_risk_index is negative
select *
from {{ ref('dim_driver') }}
where driver_safety_risk_index < 0
