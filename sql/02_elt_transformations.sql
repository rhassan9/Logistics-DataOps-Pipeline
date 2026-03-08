-- =========================================================================
-- LOGISTICS DATA WAREHOUSE: ELT MIGRATION SCRIPT
-- File: 02_elt_transformations.sql
-- Description: Extracts from raw tables, transforms via SQL, and loads into dimensional model.
-- =========================================================================

-- -------------------------------------------------------------------------
-- POPULATE DIMENSIONS
-- -------------------------------------------------------------------------

-- 1. Dim_Driver
INSERT INTO analytics.Dim_Driver (driver_id, first_name, last_name, hire_date, termination_date, is_active_driver, license_state, home_terminal, employment_status, cdl_class, years_experience)
SELECT 
    driver_id, first_name, last_name, 
    CAST(hire_date AS DATE), 
    CAST(termination_date AS DATE), 
    COALESCE(CAST(is_active_driver AS BOOLEAN), TRUE), 
    license_state, home_terminal, employment_status, cdl_class, 
    CAST(years_experience AS NUMERIC)
FROM staging.drivers
ON CONFLICT (driver_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    is_active_driver = EXCLUDED.is_active_driver;

-- Insert Ghost Dimension for UNKNOWN_DRIVER
INSERT INTO analytics.Dim_Driver (driver_id, first_name, last_name, is_active_driver)
VALUES ('UNKNOWN_DRIVER', 'Unknown', 'Driver', FALSE)
ON CONFLICT (driver_id) DO NOTHING;

-- 2. Dim_Truck
INSERT INTO analytics.Dim_Truck (truck_id, make, model_year, fuel_type, tank_capacity_gallons, status, home_terminal)
SELECT 
    truck_id, make, CAST(model_year AS INTEGER), fuel_type, 
    CAST(tank_capacity_gallons AS NUMERIC), status, home_terminal
FROM staging.trucks
ON CONFLICT (truck_id) DO UPDATE SET
    status = EXCLUDED.status;

-- Insert Ghost Dimension for UNKNOWN_TRUCK
INSERT INTO analytics.Dim_Truck (truck_id, make, status)
VALUES ('UNKNOWN_TRUCK', 'Unknown', 'Unknown')
ON CONFLICT (truck_id) DO NOTHING;

-- 3. Dim_Customer
INSERT INTO analytics.Dim_Customer (customer_id, customer_name, customer_type, credit_terms_days, primary_freight_type, account_status)
SELECT 
    customer_id, customer_name, customer_type, 
    CAST(credit_terms_days AS INTEGER), primary_freight_type, account_status
FROM staging.customers
ON CONFLICT (customer_id) DO UPDATE SET
    account_status = EXCLUDED.account_status;

-- 4. Dim_Route
INSERT INTO analytics.Dim_Route (route_id, origin_city, origin_state, destination_city, destination_state, typical_distance_miles, base_rate_per_mile)
SELECT 
    route_id, origin_city, origin_state, destination_city, destination_state, 
    CAST(typical_distance_miles AS NUMERIC), CAST(base_rate_per_mile AS NUMERIC)
FROM staging.routes
ON CONFLICT (route_id) DO NOTHING;

-- 5. Dim_Facility
INSERT INTO analytics.Dim_Facility (facility_id, facility_name, facility_type, city, state)
SELECT 
    facility_id, facility_name, facility_type, city, state
FROM staging.facilities
ON CONFLICT (facility_id) DO NOTHING;

-- Insert Ghost Dimension for UNKNOWN_FACILITY
INSERT INTO analytics.Dim_Facility (facility_id, facility_name, facility_type)
VALUES ('UNKNOWN_FACILITY', 'Unknown', 'Unknown')
ON CONFLICT (facility_id) DO NOTHING;

-- 6. Populate Dynamic Dim_Date from Dispatch Dates
INSERT INTO analytics.Dim_Date (date_sk, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, is_weekend)
SELECT DISTINCT
    CAST(TO_CHAR(CAST(dispatch_date AS DATE), 'YYYYMMDD') AS INT) AS date_sk,
    CAST(dispatch_date AS DATE) AS full_date,
    EXTRACT(YEAR FROM CAST(dispatch_date AS DATE)) AS year,
    EXTRACT(QUARTER FROM CAST(dispatch_date AS DATE)) AS quarter,
    EXTRACT(MONTH FROM CAST(dispatch_date AS DATE)) AS month,
    TO_CHAR(CAST(dispatch_date AS DATE), 'Month') AS month_name,
    EXTRACT(DAY FROM CAST(dispatch_date AS DATE)) AS day_of_month,
    EXTRACT(DOW FROM CAST(dispatch_date AS DATE)) AS day_of_week,
    TO_CHAR(CAST(dispatch_date AS DATE), 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM CAST(dispatch_date AS DATE)) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM staging.trips
WHERE dispatch_date IS NOT NULL
ON CONFLICT (full_date) DO NOTHING;

-- Ensure UNKNOWN Dates drop into a default bucket
INSERT INTO analytics.Dim_Date (date_sk, full_date, year)
VALUES (19000101, '1900-01-01', 1900)
ON CONFLICT (full_date) DO NOTHING;

-- 7. Dim_Delay_Reason (NLP Categorization)
INSERT INTO analytics.Dim_Delay_Reason (delay_reason_category)
SELECT DISTINCT "Delay_Reason"
FROM staging.maintenance_records
WHERE "Delay_Reason" IS NOT NULL
ON CONFLICT (delay_reason_category) DO NOTHING;

-- Ensure UNKNOWN drop into a default bucket
INSERT INTO analytics.Dim_Delay_Reason (delay_reason_category)
VALUES ('UNKNOWN')
ON CONFLICT (delay_reason_category) DO NOTHING;

-- 8. Dim_Incident_Category (NLP Categorization)
INSERT INTO analytics.Dim_Incident_Category (incident_category)
SELECT DISTINCT "Incident_Category"
FROM staging.safety_incidents
WHERE "Incident_Category" IS NOT NULL
ON CONFLICT (incident_category) DO NOTHING;

-- Ensure UNKNOWN drop into a default bucket
INSERT INTO analytics.Dim_Incident_Category (incident_category)
VALUES ('UNKNOWN')
ON CONFLICT (incident_category) DO NOTHING;

-- -------------------------------------------------------------------------
-- POPULATE FACT TABLES
-- -------------------------------------------------------------------------

-- 1. Fact_Shipment (Joining Loads, Trips, Delivery, Fuel, and Maintenance)
INSERT INTO analytics.Fact_Shipment (
    load_id, trip_id, driver_id, truck_id, customer_id, route_id, facility_id, dispatch_date_sk, delay_reason_sk,
    delay_duration_minutes, is_sla_breached,
    revenue, fuel_surcharge, accessorial_charges, weight_lbs,
    actual_distance_miles, actual_duration_hours, fuel_gallons_used, average_mpg, idle_time_hours,
    total_trip_cost, net_trip_margin, trip_cost_per_mile
)
WITH TripFuel AS (
    SELECT trip_id, SUM(total_cost) AS fuel_cost
    FROM staging.fuel_purchases
    GROUP BY trip_id
),
TripDelivery AS (
    SELECT trip_id, 
           MAX(actual_datetime) AS actual_arrival_time, 
           MAX(scheduled_datetime) AS expected_arrival_time,
           MAX(facility_id) AS facility_id
    FROM staging.delivery_events
    WHERE event_type = 'Delivery'
    GROUP BY trip_id
),
TripMaintenance AS (
    SELECT 
        t.trip_id,
        SUM(m.total_cost) AS maintenance_cost,
        MAX(m."Delay_Reason") AS nlp_delay_reason
    FROM staging.trips t
    JOIN staging.maintenance_records m ON t.truck_id = m.truck_id AND CAST(m.maintenance_date AS DATE) = CAST(t.dispatch_date AS DATE)
    GROUP BY t.trip_id
)
SELECT 
    l.load_id,
    t.trip_id,
    COALESCE(t.driver_id, 'UNKNOWN_DRIVER'),
    COALESCE(t.truck_id, 'UNKNOWN_TRUCK'),
    l.customer_id,
    l.route_id,
    COALESCE(td.facility_id, 'UNKNOWN_FACILITY'),
    COALESCE(CAST(TO_CHAR(CAST(t.dispatch_date AS DATE), 'YYYYMMDD') AS INT), 19000101) AS dispatch_date_sk,
    COALESCE(dr.delay_reason_sk, (SELECT delay_reason_sk FROM analytics.Dim_Delay_Reason WHERE delay_reason_category = 'UNKNOWN')),
    
    -- 1. SLA & Delivery Performance Columns
    CAST(EXTRACT(EPOCH FROM (CAST(td.actual_arrival_time AS TIMESTAMP) - CAST(td.expected_arrival_time AS TIMESTAMP))) / 60 AS INT) AS delay_duration_minutes,
    CASE WHEN CAST(td.actual_arrival_time AS TIMESTAMP) > CAST(td.expected_arrival_time AS TIMESTAMP) THEN 1 ELSE 0 END AS is_sla_breached,
    
    -- Load Measures
    CAST(l.revenue AS NUMERIC),
    CAST(l.fuel_surcharge AS NUMERIC),
    CAST(l.accessorial_charges AS NUMERIC),
    CAST(l.weight_lbs AS NUMERIC),
    
    -- Trip Measures
    CAST(t.actual_distance_miles AS NUMERIC),
    CAST(t.actual_duration_hours AS NUMERIC),
    CAST(t.fuel_gallons_used AS NUMERIC),
    CAST(t.average_mpg AS NUMERIC),
    CAST(t.idle_time_hours AS NUMERIC),
    
    -- 2. Unit Economics & Profitability Columns (Driver pay estimated at $25/hr)
    CAST(COALESCE(tf.fuel_cost, 0) + COALESCE(tm.maintenance_cost, 0) + COALESCE(t.actual_duration_hours * 25.0, 0) AS NUMERIC(10, 2)) AS total_trip_cost,
    
    CAST(CAST(l.revenue AS NUMERIC) - (COALESCE(tf.fuel_cost, 0) + COALESCE(tm.maintenance_cost, 0) + COALESCE(t.actual_duration_hours * 25.0, 0)) AS NUMERIC(10, 2)) AS net_trip_margin,
    
    CAST((COALESCE(tf.fuel_cost, 0) + COALESCE(tm.maintenance_cost, 0) + COALESCE(t.actual_duration_hours * 25.0, 0)) / NULLIF(CAST(t.actual_distance_miles AS NUMERIC), 0) AS NUMERIC(10, 2)) AS trip_cost_per_mile

FROM staging.trips t
JOIN staging.loads l ON t.load_id = l.load_id
LEFT JOIN TripFuel tf ON t.trip_id = tf.trip_id
LEFT JOIN TripDelivery td ON t.trip_id = td.trip_id
LEFT JOIN TripMaintenance tm ON t.trip_id = tm.trip_id
LEFT JOIN analytics.Dim_Delay_Reason dr ON tm.nlp_delay_reason = dr.delay_reason_category;


-- 2. Fact_Maintenance_Event
INSERT INTO analytics.Fact_Maintenance_Event (
    maintenance_id, truck_id, maintenance_date_sk,
    odometer_reading, labor_hours, labor_cost, parts_cost, total_cost, downtime_hours,
    maintenance_type, delay_reason_sk, service_description
)
SELECT 
    m.maintenance_id,
    COALESCE(m.truck_id, 'UNKNOWN_TRUCK'),
    COALESCE(CAST(TO_CHAR(CAST(m.maintenance_date AS DATE), 'YYYYMMDD') AS INT), 19000101),
    
    CAST(m.odometer_reading AS NUMERIC),
    CAST(m.labor_hours AS NUMERIC),
    CAST(m.labor_cost AS NUMERIC),
    CAST(m.parts_cost AS NUMERIC),
    CAST(m.total_cost AS NUMERIC),
    CAST(m.downtime_hours AS NUMERIC),
    
    m.maintenance_type,
    COALESCE(dr.delay_reason_sk, (SELECT delay_reason_sk FROM analytics.Dim_Delay_Reason WHERE delay_reason_category = 'UNKNOWN')),
    m.service_description
FROM staging.maintenance_records m
LEFT JOIN analytics.Dim_Delay_Reason dr ON m."Delay_Reason" = dr.delay_reason_category;


-- 3. Fact_Safety_Incident
INSERT INTO analytics.Fact_Safety_Incident (
    incident_id, driver_id, truck_id, incident_date_sk,
    at_fault_flag, preventable_flag, injury_flag,
    vehicle_damage_cost, cargo_damage_cost, claim_amount,
    incident_category_sk, incident_description
)
SELECT 
    s.incident_id,
    COALESCE(s.driver_id, 'UNKNOWN_DRIVER'),
    COALESCE(s.truck_id, 'UNKNOWN_TRUCK'),
    COALESCE(CAST(TO_CHAR(CAST(s.incident_date AS DATE), 'YYYYMMDD') AS INT), 19000101),
    
    CASE WHEN s.at_fault_flag = 'True' THEN TRUE ELSE FALSE END,
    CASE WHEN s.preventable_flag = 'True' THEN TRUE ELSE FALSE END,
    CASE WHEN s.injury_flag = 'True' THEN TRUE ELSE FALSE END,
    
    CAST(s.vehicle_damage_cost AS NUMERIC),
    CAST(s.cargo_damage_cost AS NUMERIC),
    CAST(s.claim_amount AS NUMERIC),
    
    COALESCE(ic.incident_category_sk, (SELECT incident_category_sk FROM analytics.Dim_Incident_Category WHERE incident_category = 'UNKNOWN')),
    s.description
FROM staging.safety_incidents s
LEFT JOIN analytics.Dim_Incident_Category ic ON s."Incident_Category" = ic.incident_category;
