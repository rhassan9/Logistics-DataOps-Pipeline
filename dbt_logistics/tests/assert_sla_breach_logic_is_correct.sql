-- Fails if SLA logic doesn't match the 119 minute rule
select *
from {{ ref('fact_shipment') }}
where 
    (delay_duration_minutes <= 119 and is_sla_breached = 1)
    or
    (delay_duration_minutes > 119 and is_sla_breached = 0)
