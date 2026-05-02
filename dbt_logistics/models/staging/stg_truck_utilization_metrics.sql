-- stg_truck_utilization_metrics.sql
-- Staging model for truck_utilization_metrics.csv

with source as (
    select * from {{ source('logistics_raw', 'truck_utilization_metrics') }}
),

renamed as (
    select
        cast(truck_id           as varchar)     as truck_id,
        cast(month              as date)        as month,
        cast(trips_completed    as integer)     as trips_completed,
        cast(total_miles        as numeric(12,2)) as total_miles,
        cast(total_revenue      as numeric(14,2)) as total_revenue,
        cast(average_mpg        as numeric(8,2)) as average_mpg,
        cast(maintenance_events as integer)     as maintenance_events,
        cast(maintenance_cost   as numeric(12,2)) as maintenance_cost,
        cast(downtime_hours     as numeric(8,2)) as downtime_hours,
        cast(utilization_rate   as numeric(5,4)) as utilization_rate
    from source
)

select * from renamed
