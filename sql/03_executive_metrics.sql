-- =========================================================================
-- LOGISTICS DATA WAREHOUSE: EXECUTIVE METRICS
-- File: 03_executive_metrics.sql
-- Description: Advanced analytical queries answering C-Suite business questions.
-- =========================================================================

-- -------------------------------------------------------------------------
-- Query 1: The Profit Margin & Route Optimization
-- The Question: Which routes (city pairs) generate the highest profit margin after fuel costs and penalties?
-- -------------------------------------------------------------------------

SELECT 
    r.origin_city || ', ' || r.origin_state AS origin,
    r.destination_city || ', ' || r.destination_state AS destination,
    COUNT(f.shipment_sk) AS total_shipments,
    SUM(f.revenue) AS total_revenue,
    SUM(f.total_cost) AS total_operating_cost,
    SUM(f.net_margin) AS total_profit,
    -- Profit per mile calculation
    CAST(SUM(f.net_margin) / NULLIF(SUM(f.actual_distance_miles), 0) AS NUMERIC(10, 2)) AS profit_per_mile
FROM analytics.Fact_Shipment f
JOIN analytics.Dim_Route r ON f.route_sk = r.route_sk
GROUP BY 
    r.origin_city, r.origin_state, r.destination_city, r.destination_state
HAVING 
    COUNT(f.shipment_sk) > 10 -- Filter for statistical significance (established routes)
ORDER BY 
    profit_per_mile DESC
LIMIT 10;


-- -------------------------------------------------------------------------
-- Query 2: Driver Tenure vs. On-Time Performance & Safety
-- The Question: What safety incident patterns and OTD (On-Time Delivery) rates exist by driver experience level?
-- -------------------------------------------------------------------------

WITH DriverCohorts AS (
    -- Group drivers into tenure buckets based on hire date
    SELECT 
        driver_sk,
        driver_id,
        first_name,
        last_name,
        hire_date,
        CASE 
            WHEN hire_date >= CURRENT_DATE - INTERVAL '1 year' THEN '0-1 Year (Novice)'
            WHEN hire_date >= CURRENT_DATE - INTERVAL '3 years' THEN '1-3 Years (Intermediate)'
            WHEN hire_date < CURRENT_DATE - INTERVAL '3 years' THEN '3+ Years (Veteran)'
            ELSE 'Unknown Tenure'
        END as tenure_cohort
    FROM analytics.Dim_Driver
    WHERE is_active_driver = TRUE
),
SafetyAgg AS (
    -- Aggregate safety incidents per driver
    SELECT 
        driver_sk,
        COUNT(incident_sk) as total_incidents,
        SUM(CASE WHEN preventable_flag = TRUE THEN 1 ELSE 0 END) as preventable_incidents
    FROM analytics.Fact_Safety_Incident
    GROUP BY driver_sk
),
DeliveryAgg AS (
    -- Calculate On-Time Delivery percentage per driver (joining via staging to track raw events)
    SELECT 
        t.driver_id,
        COUNT(de.event_id) AS total_deliveries,
        SUM(CASE WHEN de.on_time_flag = 'True' THEN 1 ELSE 0 END) AS on_time_deliveries
    FROM staging.delivery_events de
    JOIN staging.trips t ON de.trip_id = t.trip_id
    WHERE de.event_type = 'Delivery'
    GROUP BY t.driver_id
)
SELECT 
    c.tenure_cohort,
    COUNT(DISTINCT c.driver_sk) AS active_driver_count,
    
    -- Safety Metrics
    COALESCE(SUM(s.total_incidents), 0) AS cohort_total_incidents,
    CAST(COALESCE(SUM(s.preventable_incidents), 0) AS FLOAT) / NULLIF(COUNT(DISTINCT c.driver_sk), 0) AS preventable_incidents_per_driver,
    
    -- Delivery Performance Metrics
    SUM(d.total_deliveries) AS cohort_total_deliveries,
    CAST(SUM(d.on_time_deliveries) AS FLOAT) / NULLIF(SUM(d.total_deliveries), 0) * 100 AS on_time_delivery_rate_pct

FROM DriverCohorts c
LEFT JOIN SafetyAgg s ON c.driver_sk = s.driver_sk
LEFT JOIN DeliveryAgg d ON c.driver_id = d.driver_id
GROUP BY 
    c.tenure_cohort
ORDER BY 
    CASE 
        WHEN c.tenure_cohort = '0-1 Year (Novice)' THEN 1
        WHEN c.tenure_cohort = '1-3 Years (Intermediate)' THEN 2
        WHEN c.tenure_cohort = '3+ Years (Veteran)' THEN 3
        ELSE 4
    END;


-- -------------------------------------------------------------------------
-- Query 3: Equipment Utilization & Maintenance
-- The Question: How does truck age impact maintenance costs and downtime across seasonal patterns?
-- -------------------------------------------------------------------------

WITH MaintenanceHistory AS (
    -- Pull maintenance events with truck age details
    SELECT 
        m.truck_sk,
        t.truck_id,
        t.model_year,
        EXTRACT(YEAR FROM CURRENT_DATE) - t.model_year AS truck_age_years,
        d.year AS maintenance_year,
        d.month AS maintenance_month,
        m.total_cost,
        m.downtime_hours,
        m.nlp_delay_reason
    FROM analytics.Fact_Maintenance_Event m
    JOIN analytics.Dim_Truck t ON m.truck_sk = t.truck_sk
    JOIN analytics.Dim_Date d ON m.maintenance_date_sk = d.date_sk
    WHERE t.truck_id != 'UNKNOWN_TRUCK'
)
SELECT 
    truck_id,
    truck_age_years,
    maintenance_year,
    maintenance_month,
    
    -- Monthly aggregation
    SUM(total_cost) AS monthly_maintenance_cost,
    SUM(downtime_hours) AS monthly_downtime,
    
    -- Advanced Window Function: Cumulative running total of maintenance cost per truck over time
    SUM(SUM(total_cost)) OVER (
        PARTITION BY truck_id 
        ORDER BY maintenance_year, maintenance_month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_lifetime_maintenance_cost,
    
    -- Advanced Window Function: Rank trucks by their monthly downtime to identify chronic problem units
    RANK() OVER (
        PARTITION BY maintenance_year, maintenance_month 
        ORDER BY SUM(downtime_hours) DESC
    ) AS monthly_downtime_severity_rank,
    
    -- Identifying the primary NLP failure mode for that month
    MAX(nlp_delay_reason) AS primary_failure_category

FROM MaintenanceHistory
GROUP BY 
    truck_id, truck_age_years, maintenance_year, maintenance_month
ORDER BY 
    truck_id, maintenance_year, maintenance_month;
