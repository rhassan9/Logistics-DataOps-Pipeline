-- dim_route.sql
-- Final Route dimension with surrogate key and calculated column.
-- Calculated: route_name (pre-concatenated human-readable label for Power BI slicers)

with routes as (
    select * from {{ ref('stg_routes') }}
)

select
    -- Surrogate Key
    row_number() over (order by route_id) as route_sk,

    -- Natural Key
    route_id,

    -- Descriptive attributes
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    typical_distance_miles,
    base_rate_per_mile,
    fuel_surcharge_rate,
    typical_transit_days,

    -- CALCULATED: Human-readable route label for Power BI slicers/visuals
    -- e.g., "Chicago, IL -> Dallas, TX"
    origin_city || ', ' || origin_state || ' -> ' || destination_city || ', ' || destination_state
        as route_name

from routes
