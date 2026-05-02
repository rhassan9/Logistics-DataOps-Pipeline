-- fact_driver_monthly_snapshot.sql
-- Grain: 1 Row = 1 Month per Driver
-- Source: stg_driver_monthly_metrics

with snapshots as (
    select * from {{ ref('stg_driver_monthly_metrics') }}
),

dim_drivers as (select driver_sk, driver_id from {{ ref('dim_driver') }}),
dim_dates   as (select date_sk, full_date from {{ ref('dim_date') }})

select
    -- Foreign Keys (month_sk links to first-of-month date)
    dd.date_sk as month_sk,
    ddv.driver_sk,

    -- Measures
    s.trips_completed,
    s.total_miles,
    s.total_revenue,
    s.average_mpg,
    s.total_fuel_gallons,
    s.on_time_delivery_rate,
    s.average_idle_hours

from snapshots s

left join dim_dates dd
    on s.month = dd.full_date

left join dim_drivers ddv
    on s.driver_id = ddv.driver_id
