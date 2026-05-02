-- fact_maintenance_event.sql
-- Grain: 1 Row = 1 Repair / Preventive Maintenance Event
-- Calculated: cost_per_downtime_hour

with maintenance as (
    select * from {{ ref('stg_maintenance_records') }}
),

dim_trucks      as (select truck_sk, truck_id from {{ ref('dim_truck') }}),
dim_dates       as (select date_sk, full_date from {{ ref('dim_date') }})

select
    -- Foreign Keys
    dd.date_sk,
    dt.truck_sk,

    -- Degenerate Dimensions
    m.maintenance_id,
    m.maintenance_type,
    m.service_description,
    m.facility_location,

    -- Measures
    m.odometer_reading,
    m.labor_hours,
    m.labor_cost,
    m.parts_cost,
    m.total_cost,
    m.downtime_hours,

    -- CALCULATED: Cost efficiency of the repair (high = inefficient, e.g. 48hrs waiting for a cheap part)
    m.total_cost / nullif(m.downtime_hours, 0) as cost_per_downtime_hour

from maintenance m

left join dim_dates dd
    on m.maintenance_date = dd.full_date

left join dim_trucks dt
    on m.truck_id = dt.truck_id
