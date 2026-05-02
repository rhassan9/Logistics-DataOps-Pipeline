-- fact_fuel_purchase.sql
-- Grain: 1 Row = 1 Fuel Swipe Event
-- Source: stg_fuel_purchases

with fuel as (
    select * from {{ ref('stg_fuel_purchases') }}
),

dim_trucks    as (select truck_sk, truck_id from {{ ref('dim_truck') }}),
dim_drivers   as (select driver_sk, driver_id from {{ ref('dim_driver') }}),
dim_facilities as (
    select min(facility_sk) as facility_sk, city, state 
    from {{ ref('dim_facility') }} 
    group by city, state
),
dim_dates     as (select date_sk, full_date from {{ ref('dim_date') }})

select
    -- Foreign Keys
    dd.date_sk,
    dt.truck_sk,
    ddv.driver_sk,
    df.facility_sk,   -- NULL if stop not in our network

    -- Degenerate Dimensions
    f.fuel_purchase_id,
    f.trip_id,
    f.fuel_card_number,
    f.location_city,
    f.location_state,

    -- Measures
    f.gallons,
    f.price_per_gallon,
    f.total_cost

from fuel f

left join dim_dates dd
    on f.purchase_date = dd.full_date

left join dim_trucks dt
    on f.truck_id = dt.truck_id

left join dim_drivers ddv
    on f.driver_id = ddv.driver_id

-- Match fuel stop city/state to a known network facility (may not always match)
left join dim_facilities df
    on f.location_city = df.city and f.location_state = df.state
