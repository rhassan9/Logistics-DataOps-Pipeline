-- stg_driver_monthly_metrics.sql
-- Staging model for driver_monthly_metrics.csv

with source as (
    select * from {{ source('logistics_raw', 'driver_monthly_metrics') }}
),

renamed as (
    select
        cast(driver_id            as varchar)     as driver_id,
        cast(month                as date)        as month,
        cast(trips_completed      as integer)     as trips_completed,
        cast(total_miles          as numeric(12,2)) as total_miles,
        cast(total_revenue        as numeric(14,2)) as total_revenue,
        cast(average_mpg          as numeric(8,2)) as average_mpg,
        cast(total_fuel_gallons   as numeric(10,2)) as total_fuel_gallons,
        cast(on_time_delivery_rate as numeric(5,4)) as on_time_delivery_rate,
        cast(average_idle_hours   as numeric(8,2)) as average_idle_hours
    from source
)

select * from renamed
