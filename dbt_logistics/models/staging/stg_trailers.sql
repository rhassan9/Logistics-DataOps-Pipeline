-- stg_trailers.sql
-- Staging model for trailers.csv

with source as (
    select * from {{ source('logistics_raw', 'trailers') }}
),

renamed as (
    select
        cast(trailer_id        as varchar)  as trailer_id,
        cast(trailer_number    as varchar)  as trailer_number,
        cast(trailer_type      as varchar)  as trailer_type,
        cast(length_feet       as integer)  as length_feet,
        cast(model_year        as integer)  as model_year,
        cast(vin               as varchar)  as vin,
        cast(acquisition_date  as date)     as acquisition_date,
        cast(status            as varchar)  as status,
        cast(current_location  as varchar)  as current_location
    from source
)

select * from renamed
