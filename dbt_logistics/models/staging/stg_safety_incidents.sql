-- stg_safety_incidents.sql
-- Staging model for clean_safety_incidents.csv

with source as (
    select * from {{ source('logistics_raw', 'clean_safety_incidents') }}
),

renamed as (
    select
        cast(incident_id          as varchar)     as incident_id,
        cast(trip_id              as varchar)     as trip_id,
        cast(truck_id             as varchar)     as truck_id,
        cast(driver_id            as varchar)     as driver_id,
        cast(incident_date        as date)        as incident_date,
        cast(incident_type        as varchar)     as incident_type,
        cast(location_city        as varchar)     as location_city,
        cast(location_state       as varchar)     as location_state,
        cast(at_fault_flag        as smallint)    as at_fault_flag,
        cast(injury_flag          as smallint)    as injury_flag,
        cast(vehicle_damage_cost  as numeric(12,2)) as vehicle_damage_cost,
        cast(cargo_damage_cost    as numeric(12,2)) as cargo_damage_cost,
        cast(claim_amount         as numeric(12,2)) as claim_amount,
        cast(preventable_flag     as smallint)    as preventable_flag,
        cast(description          as text)        as description,
        cast(incident_category    as varchar)     as incident_category
    from source
)

select * from renamed
