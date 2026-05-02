-- stg_maintenance_records.sql
-- Staging model for clean_maintenance_records.csv

with source as (
    select * from {{ source('logistics_raw', 'clean_maintenance_records') }}
),

renamed as (
    select
        cast(maintenance_id    as varchar)     as maintenance_id,
        cast(truck_id          as varchar)     as truck_id,
        cast(maintenance_date  as date)        as maintenance_date,
        cast(maintenance_type  as varchar)     as maintenance_type,
        cast(odometer_reading  as numeric(12,2)) as odometer_reading,
        cast(labor_hours       as numeric(8,2)) as labor_hours,
        cast(labor_cost        as numeric(12,2)) as labor_cost,
        cast(parts_cost        as numeric(12,2)) as parts_cost,
        cast(total_cost        as numeric(12,2)) as total_cost,
        cast(downtime_hours    as numeric(8,2)) as downtime_hours,
        cast(facility_location as varchar)     as facility_location,
        cast(service_description as text)      as service_description,
        cast(delay_reason      as varchar)     as delay_reason
    from source
)

select * from renamed
