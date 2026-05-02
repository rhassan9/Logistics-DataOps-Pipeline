-- dim_trailer.sql
-- Final Trailer dimension with surrogate key. No calculated columns required.

with trailers as (
    select * from {{ ref('stg_trailers') }}
)

select
    -- Surrogate Key
    row_number() over (order by trailer_id) as trailer_sk,

    -- Natural Key
    trailer_id,

    -- Descriptive attributes
    trailer_number,
    trailer_type,
    length_feet,
    model_year,
    vin,
    acquisition_date,
    status,
    current_location

from trailers
