-- stg_safety_incidents.sql
-- Staging model for clean_safety_incidents.csv

with source as (
    select * from {{ source('logistics_raw', 'safety_incidents') }}
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
        cast(cast(at_fault_flag as integer) as smallint)    as at_fault_flag,
        cast(cast(injury_flag as integer) as smallint)    as injury_flag,
        cast(vehicle_damage_cost  as numeric(12,2)) as vehicle_damage_cost,
        cast(cargo_damage_cost    as numeric(12,2)) as cargo_damage_cost,
        cast(claim_amount         as numeric(12,2)) as claim_amount,
        cast(cast(preventable_flag as integer) as smallint)    as preventable_flag,
        cast(description          as text)        as description,
        cast("Incident_Category"    as varchar)     as incident_category
    from source
)

select * from renamed
