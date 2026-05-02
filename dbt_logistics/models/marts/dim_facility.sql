-- dim_facility.sql
-- Final Facility dimension with surrogate key. No calculated columns required.

with facilities as (
    select * from {{ ref('stg_facilities') }}
)

select
    row_number() over (order by facility_id) as facility_sk,
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    latitude,
    longitude,
    dock_doors,
    operating_hours
from facilities
