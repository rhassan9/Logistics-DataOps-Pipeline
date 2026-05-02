-- stg_facilities.sql
-- Staging model for facilities.csv

with source as (
    select * from {{ source('logistics_raw', 'facilities') }}
),

renamed as (
    select
        cast(facility_id      as varchar)     as facility_id,
        cast(facility_name    as varchar)     as facility_name,
        cast(facility_type    as varchar)     as facility_type,
        cast(city             as varchar)     as city,
        cast(state            as varchar)     as state,
        cast(latitude         as numeric(9,6)) as latitude,
        cast(longitude        as numeric(9,6)) as longitude,
        cast(dock_doors       as integer)     as dock_doors,
        cast(operating_hours  as varchar)     as operating_hours
    from source
)

select * from renamed
