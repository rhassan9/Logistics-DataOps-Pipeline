-- stg_routes.sql
-- Staging model for routes.csv

with source as (
    select * from {{ source('logistics_raw', 'routes') }}
),

renamed as (
    select
        cast(route_id               as varchar)     as route_id,
        cast(origin_city            as varchar)     as origin_city,
        cast(origin_state           as varchar)     as origin_state,
        cast(destination_city       as varchar)     as destination_city,
        cast(destination_state      as varchar)     as destination_state,
        cast(typical_distance_miles as numeric(10,2)) as typical_distance_miles,
        cast(base_rate_per_mile     as numeric(8,4)) as base_rate_per_mile,
        cast(fuel_surcharge_rate    as numeric(8,4)) as fuel_surcharge_rate,
        cast(typical_transit_days   as integer)     as typical_transit_days
    from source
)

select * from renamed
