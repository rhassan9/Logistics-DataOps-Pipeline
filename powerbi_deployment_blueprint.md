# Logistics Operations Data Pipeline: Power BI Deployment Blueprint

This blueprint outlines the final deployment strategy for rendering the Logistics Data Warehouse into an interactive, enterprise-grade Power BI application.

## 1. Import-Mode Analytical Queries
Given the size of the dataset and the complexity of the DAX measures, we recommend using **Import Mode** via the VertiPaq engine for lightning-fast analytics.

*   **Database Connection**: Connect Power BI to the underlying PostgreSQL database using the standard PostgreSQL connector.
*   **Data Model Setup**: Map the Kimball Star Schema built by the `dbt_logistics/` project (staging + mart models).
*   **Relationships** (All `Many-to-One`, Single Direction):
    1.  `Fact_Shipment[date_sk]` -> `Dim_Date[date_sk]`
    2.  `Fact_Shipment[facility_sk]` -> `Dim_Facility[facility_sk]`
    3.  `Fact_Shipment[truck_sk]` -> `Dim_Truck[truck_sk]`
    4.  `Fact_Shipment[driver_sk]` -> `Dim_Driver[driver_sk]`
    5.  `Fact_Shipment[route_sk]` -> `Dim_Route[route_sk]`
    6.  `Fact_Shipment[customer_sk]` -> `Dim_Customer[customer_sk]`
    7.  `Fact_Shipment[trailer_sk]` -> `Dim_Trailer[trailer_sk]`
    8.  `Fact_Shipment[incident_category_sk]` -> `Dim_Incident_Category[incident_category_sk]`
    9.  `Fact_Safety_Incident[incident_category_sk]` -> `Dim_Incident_Category[incident_category_sk]`
    10. `Fact_Maintenance_Event[truck_sk]` -> `Dim_Truck[truck_sk]`
    11. `Fact_Fuel_Purchase[truck_sk]` -> `Dim_Truck[truck_sk]`

## 2. Core Visual Pages Design

### Page 1: The C-Suite Summary
*   **Target Audience**: Executive Leadership
*   **Key Visuals**:
    *   **KPI Cards**: Total Revenue, Average CPM (Cost Per Mile), Total Profit, Overall SLA Compliance %.
    *   **Line Chart**: Timeline of Total Volume vs. SLA Compliance over time (using `Dim_Date[full_date]`).
    *   **Map Visual**: Facility Locations sized by Volume, colored conditionally by Profit Margin (Green = High Margin, Red = Low Margin).
    *   **Matrix**: Top and Bottom 5 Routes by `net_trip_margin`.
    *   **Time Intelligence Cards**: YTD Revenue, Revenue YoY % Change.

### Page 2: Operations & NLP Root Cause
*   **Target Audience**: Operational Managers
*   **Key Visuals**:
    *   **Pareto Chart**: Delay Incident Counts broken down by `Dim_Incident_Category[incident_category]`. Identifies the 20% of delay reasons causing 80% of SLA breaches.
    *   **Donut Chart (The Ghost Dimension)**: Shows % of Delays attributed to Unallocated Assets (`UNKNOWN_TRUCK` / `UNKNOWN_DRIVER`).
    *   **Driver Metric Table**: Ranking of Drivers by their on-time delivery percentages and `driver_safety_risk_index`.
    *   **Trend Cards**: Revenue MoM % Change, SLA MoM Variance.

### Page 3: Asset Utilization
*   **Target Audience**: Fleet Maintenance & Safety teams
*   **Key Visuals**:
    *   **Scatter Plot**: Correlating `Dim_Truck[truck_age_years]` versus Total Maintenance Cost.
    *   **Decomposition Tree**: Tracing `total_trip_cost` down to Equipment Downtime or specific Safety Damages.
    *   **Bar Chart**: Total Incident Damage Cost split by `Dim_Incident_Category[incident_category]`.
    *   **Flag Table**: Underutilized assets flagged by `Dim_Truck[is_underutilized_asset]`.

## 3. Drill-Through and Row-Level Security (RLS) Layers

### Row-Level Security (RLS)
To ensure regional managers only see their pertinent data:
1.  **Role Setup**: Create roles inside Power BI Desktop (e.g., `Manager_East_Coast`).
2.  **DAX Filter**: Apply a DAX filter on the `Dim_Facility` table:
    ```dax
    [region] = "East"
    ```
    or dynamically based on the logged-in user:
    ```dax
    [manager_email] = USERPRINCIPALNAME()
    ```

### Drill-Through Configuration
Configure a hidden "Trip Details" page for granular, shipment-level auditing.
1.  **Target Page Setup**: Create a new page named `Trip Details`. Add detailed table visuals containing `Fact_Shipment[trip_id]`, timestamps, costs, and delay variables.
2.  **Drill-Through Fields**: Add `Dim_Facility[facility_name]` or `Dim_Truck[truck_id]` to the Drill-through filters well on this target page.
3.  **User Experience**: End-users can now right-click a high-problem facility on the C-Suite Map and select "Drill through -> Trip Details" to immediately audit the corresponding line-item anomalies.
