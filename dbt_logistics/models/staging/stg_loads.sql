-- stg_loads.sql
-- Staging model for loads.csv
-- Standardizes types for the freight contract/billing source table.

with source as (
    select * from {{ source('logistics_raw', 'loads') }}
),

renamed as (
    select
        cast(load_id           as varchar)     as load_id,
        cast(customer_id       as varchar)     as customer_id,
        cast(route_id          as varchar)     as route_id,
        cast(load_date         as date)        as load_date,
        cast(load_type         as varchar)     as load_type,
        cast(booking_type      as varchar)     as booking_type,
        cast(load_status       as varchar)     as load_status,
        cast(weight_lbs        as numeric(10,2)) as weight_lbs,
        cast(pieces            as integer)     as pieces,
        cast(revenue           as numeric(12,2)) as revenue,
        cast(fuel_surcharge    as numeric(12,2)) as fuel_surcharge,
        cast(accessorial_charges as numeric(12,2)) as accessorial_charges
    from source
)

select * from renamed
