-- stg_drivers.sql
-- Staging model for clean_drivers.csv

with source as (
    select * from {{ source('logistics_raw', 'drivers') }}
),

renamed as (
    select
        cast(driver_id          as varchar)  as driver_id,
        cast(first_name         as varchar)  as first_name,
        cast(last_name          as varchar)  as last_name,
        cast(hire_date          as date)     as hire_date,
        cast(termination_date   as date)     as termination_date,
        cast(license_number     as varchar)  as license_number,
        cast(license_state      as varchar)  as license_state,
        cast(date_of_birth      as date)     as date_of_birth,
        cast(home_terminal      as varchar)  as home_terminal,
        cast(employment_status  as varchar)  as employment_status,
        cast(cdl_class          as varchar)  as cdl_class,
        cast(years_experience   as integer)  as years_experience,
        cast(cast(is_active_driver as integer) as smallint) as is_active_driver
    from source
)

select * from renamed
