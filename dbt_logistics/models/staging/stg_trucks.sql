-- stg_trucks.sql
-- Staging model for trucks.csv

with source as (
    select * from {{ source('logistics_raw', 'trucks') }}
),

renamed as (
    select
        cast(truck_id              as varchar)     as truck_id,
        cast(unit_number           as varchar)     as unit_number,
        cast(make                  as varchar)     as make,
        cast(model_year            as integer)     as model_year,
        cast(vin                   as varchar)     as vin,
        cast(acquisition_date      as date)        as acquisition_date,
        cast(acquisition_mileage   as numeric(12,2)) as acquisition_mileage,
        cast(fuel_type             as varchar)     as fuel_type,
        cast(tank_capacity_gallons as numeric(8,2)) as tank_capacity_gallons,
        cast(status                as varchar)     as status,
        cast(home_terminal         as varchar)     as home_terminal
    from source
),

ghost as (
    select
        cast('UNKNOWN_TRUCK' as varchar)     as truck_id,
        cast('UNKNOWN' as varchar)           as unit_number,
        cast('Unknown' as varchar)           as make,
        cast(1970 as integer)                as model_year,
        cast('UNKNOWN' as varchar)           as vin,
        cast('1970-01-01' as date)           as acquisition_date,
        cast(0 as numeric(12,2))             as acquisition_mileage,
        cast('Unknown' as varchar)           as fuel_type,
        cast(0 as numeric(8,2))              as tank_capacity_gallons,
        cast('Unknown' as varchar)           as status,
        cast('UNKNOWN' as varchar)           as home_terminal
)

select * from renamed
union all
select * from ghost
