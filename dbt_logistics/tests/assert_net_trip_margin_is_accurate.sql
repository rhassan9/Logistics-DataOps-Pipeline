-- Fails if net_trip_margin is not exactly revenue minus costs
select *
from {{ ref('fact_shipment') }}
where abs(net_trip_margin - (total_billed_revenue - total_trip_cost)) > 0.05
