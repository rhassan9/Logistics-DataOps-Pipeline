-- stg_trips.sql
-- Staging model for clean_trips.csv
-- Standardizes column types and names before joining into Fact_Shipment.

with source as (
    select * from {{ source('logistics_raw', 'clean_trips') }}
),

renamed as (
    select
        -- Identifiers
        cast(trip_id    as varchar)  as trip_id,
        cast(load_id    as varchar)  as load_id,
        cast(driver_id  as varchar)  as driver_id,
        cast(truck_id   as varchar)  as truck_id,
        cast(trailer_id as varchar)  as trailer_id,

        -- Dates
        cast(dispatch_date as date)  as dispatch_date,

        -- Metrics
        cast(actual_distance_miles  as numeric(10,2)) as actual_distance_miles,
        cast(actual_duration_hours  as numeric(10,2)) as actual_duration_hours,
        cast(fuel_gallons_used      as numeric(10,2)) as fuel_gallons_used,
        cast(average_mpg            as numeric(10,2)) as average_mpg,
        cast(idle_time_hours        as numeric(10,2)) as idle_time_hours,

        -- Status
        cast(trip_status as varchar) as trip_status

    from source
)

select * from renamed
