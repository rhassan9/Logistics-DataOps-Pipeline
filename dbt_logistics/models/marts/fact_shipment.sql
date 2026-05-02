-- fact_shipment.sql
-- The core fact table. Grain: 1 Row = 1 Delivered Freight Load.
-- Sources: loads + trips + delivery_events
-- Calculated Columns: total_billed_revenue, total_trip_cost, net_trip_margin,
--   runtime_efficiency_ratio, trip_revenue_per_mile, trip_cost_per_mile,
--   driver_efficiency_score, fuel_efficiency_divergence,
--   delay_duration_minutes, is_sla_breached

with loads as (
    select * from {{ ref('stg_loads') }}
),

trips as (
    select * from {{ ref('stg_trips') }}
),

-- Get the scheduled/actual arrival event for each trip (used for SLA calculation)
arrival_events as (
    select
        trip_id,
        scheduled_datetime,
        actual_datetime,
        detention_minutes,
        facility_id    as dest_facility_id,
        is_telematics_drop
    from {{ ref('stg_delivery_events') }}
    where event_type = 'Arrival'
),

-- Aggregate direct fuel costs per trip from exact receipts
fuel_costs as (
    select
        trip_id,
        sum(total_cost) as trip_fuel_cost
    from {{ ref('stg_fuel_purchases') }}
    group by trip_id
),

-- Aggregate exact safety costs per trip (vehicle + cargo damage minus insurance reimbursement)
-- claim_amount is subtracted as it represents a cost-recovery insurance payout
safety_costs as (
    select
        trip_id,
        sum(vehicle_damage_cost + cargo_damage_cost - claim_amount) as trip_safety_cost
    from {{ ref('stg_safety_incidents') }}
    group by trip_id
),

-- Pull historical average MPG per truck from the monthly snapshot
-- to use as the baseline for fuel_efficiency_divergence
truck_baseline_mpg as (
    select
        truck_id,
        avg(average_mpg) as historical_avg_mpg
    from {{ ref('stg_truck_utilization_metrics') }}
    group by truck_id
),

-- Join dimensions for surrogate key lookups
dim_drivers   as (select driver_sk, driver_id from {{ ref('dim_driver') }}),
dim_trucks    as (select truck_sk, truck_id from {{ ref('dim_truck') }}),
dim_trailers  as (select trailer_sk, trailer_id from {{ ref('dim_trailer') }}),
dim_customers as (select customer_sk, customer_id from {{ ref('dim_customer') }}),
-- Get the hardcoded SKs for our logical defaults
dim_incident_categories_defaults as (
    select
        max(case when incident_category = 'Unknown / Unreported Delay' then incident_category_sk end) as sk_unknown_delay,
        max(case when incident_category = 'No Delay' then incident_category_sk end) as sk_no_delay
    from {{ ref('dim_incident_category') }}
),

dim_incident_categories_safety as (
    select incident_category_sk, incident_category
    from {{ ref('dim_incident_category') }}
),
dim_routes    as (select route_sk, route_id, base_rate_per_mile from {{ ref('dim_route') }}),
dim_facilities as (select facility_sk, facility_id from {{ ref('dim_facility') }}),
dim_dates     as (select date_sk, full_date from {{ ref('dim_date') }})

select
    -- -----------------------------------------------
    -- Foreign Keys -> Dimensions
    -- -----------------------------------------------
    dd.date_sk,
    dc.customer_sk,
    dr.route_sk,
    ddv.driver_sk,
    dt.truck_sk,
    dtr.trailer_sk,
    df.facility_sk,

    -- -----------------------------------------------
    -- Degenerate Dimensions (Line-Item IDs)
    -- -----------------------------------------------
    l.load_id,
    t.trip_id,
    l.load_type,
    l.booking_type,

    -- -----------------------------------------------
    -- Base Measures / Metrics
    -- -----------------------------------------------
    l.weight_lbs,
    l.pieces,
    t.actual_distance_miles,
    t.actual_duration_hours,
    ae.detention_minutes,
    t.fuel_gallons_used,
    t.average_mpg,
    t.idle_time_hours,
    l.revenue,
    l.fuel_surcharge,
    l.accessorial_charges,
    ae.is_telematics_drop,

    -- -----------------------------------------------
    -- CALCULATED COLUMNS
    -- -----------------------------------------------

    -- Total revenue billed to customer for this load
    l.revenue + l.fuel_surcharge + l.accessorial_charges
        as total_billed_revenue,

    -- Total precise cost: route base + exact fuel receipts + exact safety costs
    -- NOTE: Maintenance excluded — no trip_id on maintenance records
    (t.actual_distance_miles * dr.base_rate_per_mile)
        + coalesce(fc.trip_fuel_cost, 0)
        + coalesce(sc.trip_safety_cost, 0)
        as total_trip_cost,

    -- Net profit for the run
    (l.revenue + l.fuel_surcharge + l.accessorial_charges)
    - ((t.actual_distance_miles * dr.base_rate_per_mile)
        + coalesce(fc.trip_fuel_cost, 0)
        + coalesce(sc.trip_safety_cost, 0))
        as net_trip_margin,

    -- Efficiency ratio: actual time vs expected time at 50mph baseline
    -- > 1.0 = slower than expected
    t.actual_duration_hours / nullif(t.actual_distance_miles / 50.0, 0)
        as runtime_efficiency_ratio,

    -- Revenue earned per mile driven
    (l.revenue + l.fuel_surcharge + l.accessorial_charges)
        / nullif(t.actual_distance_miles, 0)
        as trip_revenue_per_mile,

    -- Cost per mile driven
    ((t.actual_distance_miles * dr.base_rate_per_mile)
        + coalesce(fc.trip_fuel_cost, 0)
        + coalesce(sc.trip_safety_cost, 0))
        / nullif(t.actual_distance_miles, 0)
        as trip_cost_per_mile,

    -- Explicit Incident Category mapping based on SLA Logic
    case
        -- 1. SLA NOT breached -> 'No Delay'
        when extract(epoch from (ae.actual_datetime - ae.scheduled_datetime)) / 60.0 <= 119
             then (select sk_no_delay from dim_incident_categories_defaults)

        -- 2. SLA breached AND Safety Incident exists -> Specific Safety SK
        when extract(epoch from (ae.actual_datetime - ae.scheduled_datetime)) / 60.0 > 119
             and dic_safety.incident_category_sk is not null
             then dic_safety.incident_category_sk

        -- 3. SLA breached AND NO Safety Incident -> 'Unknown / Unreported'
        when extract(epoch from (ae.actual_datetime - ae.scheduled_datetime)) / 60.0 > 119
             and dic_safety.incident_category_sk is null
             then (select sk_unknown_delay from dim_incident_categories_defaults)

    end as incident_category_sk,

    -- Delay duration in minutes: actual arrival vs scheduled arrival
    -- Can be negative (early arrival)
    extract(epoch from (ae.actual_datetime - ae.scheduled_datetime)) / 60.0
        as delay_duration_minutes,

    -- SLA breach flag: >119 minutes late = breached (standard 2-hour grace window)
    case
        when extract(epoch from (ae.actual_datetime - ae.scheduled_datetime)) / 60.0 > 119
        then 1 else 0
    end as is_sla_breached,

    -- Fuel efficiency divergence: this trip's MPG vs truck's historical average
    -- Positive = more efficient than usual; Negative = worse than usual
    t.average_mpg - coalesce(bm.historical_avg_mpg, t.average_mpg)
        as fuel_efficiency_divergence,

    -- Driver efficiency score: net margin zeroed out if SLA was breached
    ((l.revenue + l.fuel_surcharge + l.accessorial_charges)
        - ((t.actual_distance_miles * dr.base_rate_per_mile)
            + coalesce(fc.trip_fuel_cost, 0)
            + coalesce(sc.trip_safety_cost, 0)))
    * (1 - case
        when extract(epoch from (ae.actual_datetime - ae.scheduled_datetime)) / 60.0 > 119
        then 1 else 0
       end)
        as driver_efficiency_score

from loads l

-- Join trips on shared load_id
inner join trips t
    on l.load_id = t.load_id

-- Join delivery events (arrival only) for SLA calculation
left join arrival_events ae
    on t.trip_id = ae.trip_id

-- Join aggregated parallel costs
left join fuel_costs fc
    on t.trip_id = fc.trip_id

left join safety_costs sc
    on t.trip_id = sc.trip_id

-- Join to safety incident categories only (for case logic)
left join dim_incident_categories_safety dic_safety
    on sc.primary_incident_category = dic_safety.incident_category

-- Join truck baseline MPG
left join truck_baseline_mpg bm
    on t.truck_id = bm.truck_id

-- Dimension joins for surrogate keys
left join dim_drivers ddv
    on t.driver_id = ddv.driver_id

left join dim_trucks dt
    on t.truck_id = dt.truck_id

left join dim_trailers dtr
    on t.trailer_id = dtr.trailer_id

left join dim_customers dc
    on l.customer_id = dc.customer_id

left join dim_routes dr
    on l.route_id = dr.route_id

left join dim_facilities df
    on ae.dest_facility_id = df.facility_id

left join dim_dates dd
    on l.load_date = dd.full_date
