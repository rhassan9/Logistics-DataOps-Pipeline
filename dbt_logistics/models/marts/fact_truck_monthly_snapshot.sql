-- fact_truck_monthly_snapshot.sql
-- Grain: 1 Row = 1 Month per Truck
-- Source: stg_truck_utilization_metrics
-- NOTE: This snapshot is also used as the historical MPG baseline in fact_shipment.sql

with snapshots as (
    select * from {{ ref('stg_truck_utilization_metrics') }}
),

dim_trucks  as (select truck_sk, truck_id from {{ ref('dim_truck') }}),
dim_dates   as (select date_sk, full_date from {{ ref('dim_date') }})

select
    -- Foreign Keys
    dd.date_sk as month_sk,
    dt.truck_sk,

    -- Measures
    s.trips_completed,
    s.total_miles,
    s.total_revenue,
    s.average_mpg,
    s.maintenance_events,
    s.maintenance_cost,
    s.downtime_hours,
    s.utilization_rate

from snapshots s

left join dim_dates dd
    on s.month = dd.full_date

left join dim_trucks dt
    on s.truck_id = dt.truck_id
