-- =========================================================================
-- LOGISTICS DATA WAREHOUSE: STAR SCHEMA DDL
-- File: 01_warehouse_ddl.sql
-- Description: Creates the OLAP Dimension and Fact tables for PowerBI.
-- =========================================================================

-- -------------------------------------------------------------------------
-- 0. SCHEMA INITIALIZATION
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;

-- -------------------------------------------------------------------------
-- 1. DIMENSION TABLES
-- -------------------------------------------------------------------------

-- Dim_Driver
CREATE TABLE IF NOT EXISTS analytics.Dim_Driver (
    driver_sk SERIAL PRIMARY KEY,
    driver_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    hire_date DATE,
    termination_date DATE,
    is_active_driver BOOLEAN,
    license_state VARCHAR(2),
    home_terminal VARCHAR(100),
    employment_status VARCHAR(50),
    cdl_class VARCHAR(10),
    years_experience NUMERIC
);

-- Dim_Truck
CREATE TABLE IF NOT EXISTS analytics.Dim_Truck (
    truck_sk SERIAL PRIMARY KEY,
    truck_id VARCHAR(50) UNIQUE NOT NULL,
    make VARCHAR(50),
    model_year INTEGER,
    fuel_type VARCHAR(50),
    tank_capacity_gallons NUMERIC,
    status VARCHAR(50),
    home_terminal VARCHAR(100)
);

-- Dim_Customer
CREATE TABLE IF NOT EXISTS analytics.Dim_Customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(255),
    customer_type VARCHAR(100),
    credit_terms_days INTEGER,
    primary_freight_type VARCHAR(100),
    account_status VARCHAR(50)
);

-- Dim_Route
CREATE TABLE IF NOT EXISTS analytics.Dim_Route (
    route_sk SERIAL PRIMARY KEY,
    route_id VARCHAR(50) UNIQUE NOT NULL,
    origin_city VARCHAR(100),
    origin_state VARCHAR(2),
    destination_city VARCHAR(100),
    destination_state VARCHAR(2),
    typical_distance_miles NUMERIC,
    base_rate_per_mile NUMERIC
);

-- Dim_Facility
CREATE TABLE IF NOT EXISTS analytics.Dim_Facility (
    facility_sk SERIAL PRIMARY KEY,
    facility_id VARCHAR(50) UNIQUE NOT NULL,
    facility_name VARCHAR(255),
    facility_type VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(2)
);

-- Dim_Date (Standard Calendar Dimension)
CREATE TABLE IF NOT EXISTS analytics.Dim_Date (
    date_sk INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- Dim_Delay_Reason (NLP Categorizations)
CREATE TABLE IF NOT EXISTS analytics.Dim_Delay_Reason (
    delay_reason_sk SERIAL PRIMARY KEY,
    delay_reason_category VARCHAR(100) UNIQUE NOT NULL
);

-- -------------------------------------------------------------------------
-- 2. FACT TABLES
-- -------------------------------------------------------------------------

-- Fact_Shipment (Core Grain: 1 Trip = 1 Shipment)
CREATE TABLE IF NOT EXISTS analytics.Fact_Shipment (
    shipment_sk SERIAL PRIMARY KEY,
    load_id VARCHAR(50),      -- Degenerate Dimension
    trip_id VARCHAR(50),      -- Degenerate Dimension
    
    -- Foreign Keys
    driver_sk INT REFERENCES analytics.Dim_Driver(driver_sk),
    truck_sk INT REFERENCES analytics.Dim_Truck(truck_sk),
    customer_sk INT REFERENCES analytics.Dim_Customer(customer_sk),
    route_sk INT REFERENCES analytics.Dim_Route(route_sk),
    dispatch_date_sk INT REFERENCES analytics.Dim_Date(date_sk),
    delay_reason_sk INT REFERENCES analytics.Dim_Delay_Reason(delay_reason_sk),
    
    -- SLA & Delivery Performance
    delay_duration_minutes INT,
    is_sla_breached INT,
    
    -- Load Measures
    revenue NUMERIC(10, 2),
    fuel_surcharge NUMERIC(10, 2),
    accessorial_charges NUMERIC(10, 2),
    weight_lbs NUMERIC(10, 2),
    
    -- Trip Measures
    actual_distance_miles NUMERIC(10, 2),
    actual_duration_hours NUMERIC(10, 2),
    fuel_gallons_used NUMERIC(10, 2),
    average_mpg NUMERIC(10, 2),
    idle_time_hours NUMERIC(10, 2),
    
    -- Unit Economics & Profitability
    total_trip_cost NUMERIC(10, 2),
    net_trip_margin NUMERIC(10, 2),
    trip_cost_per_mile NUMERIC(10, 2)
);

-- Fact_Maintenance_Event
CREATE TABLE IF NOT EXISTS analytics.Fact_Maintenance_Event (
    maintenance_sk SERIAL PRIMARY KEY,
    maintenance_id VARCHAR(50), -- Degenerate Dimension
    
    -- Foreign Keys
    truck_sk INT REFERENCES analytics.Dim_Truck(truck_sk),
    maintenance_date_sk INT REFERENCES analytics.Dim_Date(date_sk),
    
    -- Measures
    odometer_reading NUMERIC,
    labor_hours NUMERIC(10, 2),
    labor_cost NUMERIC(10, 2),
    parts_cost NUMERIC(10, 2),
    total_cost NUMERIC(10, 2),
    downtime_hours NUMERIC(10, 2),
    
    -- NLP Categorizations
    maintenance_type VARCHAR(100),
    nlp_delay_reason VARCHAR(100)
);

-- Fact_Safety_Incident
CREATE TABLE IF NOT EXISTS analytics.Fact_Safety_Incident (
    incident_sk SERIAL PRIMARY KEY,
    incident_id VARCHAR(50), -- Degenerate Dimension
    
    -- Foreign Keys
    driver_sk INT REFERENCES analytics.Dim_Driver(driver_sk),
    truck_sk INT REFERENCES analytics.Dim_Truck(truck_sk),
    incident_date_sk INT REFERENCES analytics.Dim_Date(date_sk),
    
    -- Status Flags
    at_fault_flag BOOLEAN,
    preventable_flag BOOLEAN,
    injury_flag BOOLEAN,
    
    -- Measures
    vehicle_damage_cost NUMERIC(10, 2),
    cargo_damage_cost NUMERIC(10, 2),
    claim_amount NUMERIC(10, 2),
    
    -- NLP Categorizations
    nlp_incident_category VARCHAR(100)
);
