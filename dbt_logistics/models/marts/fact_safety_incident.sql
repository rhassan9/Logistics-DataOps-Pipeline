-- fact_safety_incident.sql
-- Grain: 1 Row = 1 Accident / Traffic Violation / DOT Citation
-- Source: stg_safety_incidents

with incidents as (
    select * from {{ ref('stg_safety_incidents') }}
),

dim_trucks      as (select truck_sk, truck_id from {{ ref('dim_truck') }}),
dim_drivers     as (select driver_sk, driver_id from {{ ref('dim_driver') }}),
dim_incident_categories as (select incident_category_sk, incident_category, incident_type from {{ ref('dim_incident_category') }}),
dim_dates       as (select date_sk, full_date from {{ ref('dim_date') }})

select
    -- Foreign Keys
    dd.date_sk,
    dt.truck_sk,
    ddv.driver_sk,
    dic.incident_category_sk,

    -- Degenerate Dimensions
    i.incident_id,
    i.trip_id,
    i.location_city,
    i.location_state,
    i.description,

    -- Measures / Flags
    i.preventable_flag,
    i.vehicle_damage_cost,
    i.cargo_damage_cost,
    i.claim_amount

from incidents i

left join dim_dates dd
    on i.incident_date = dd.full_date

left join dim_trucks dt
    on i.truck_id = dt.truck_id

left join dim_drivers ddv
    on i.driver_id = ddv.driver_id

left join dim_incident_categories dic
    on i.incident_category = dic.incident_category
   and coalesce(i.incident_type, 'None') = coalesce(dic.incident_type, 'None')
