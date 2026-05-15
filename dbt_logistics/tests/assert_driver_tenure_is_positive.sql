-- Fails if tenure is negative
select *
from {{ ref('dim_driver') }}
where driver_tenure_days < 0
