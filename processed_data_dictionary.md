# Logistics Data Pipeline - Processed Files Dictionary

This document outlines the schema, column names, and their purposes for every file output into the `data/processed/` folder. These clean records act as the source for your database schemas and Power BI reporting.

---

### 1. `clean_delivery_events.csv`
**Total Columns:** 12
**Details:** Tracks timestamps and locations of specific logistical events (departures, arrivals).
*   `event_id`: Unique identifier for the chronological event.
*   `load_id`: References the overall freight load.
*   `trip_id`: References the specific dispatched trip.
*   `event_type`: Categorical label (e.g., 'Arrival', 'Departure', 'Delay').
*   `facility_id`: Unifies event location with the Facility dimension.
*   `scheduled_datetime`: The expected ISO-8601 time for the event.
*   `actual_datetime`: The physical recorded time for the event.
*   `detention_minutes`: How long the truck was waiting at the facility.
*   `on_time_flag`: Boolean/binary indicating if the step met SLA timing.
*   `location_city`: Geographical city of the event.
*   `location_state`: Geographical state.
*   `is_telematics_drop`: Boolean flag denoting if the physical GPS sensor dropped/failed to record actual time.

---

### 2. `clean_drivers.csv`
**Total Columns:** 13
**Details:** Personnel dimension containing data about the fleet operators.
*   `driver_id`: Unique identifier for the driver.
*   `first_name`: Driver's first name.
*   `last_name`: Driver's last name.
*   `hire_date`: Date the driver started employment.
*   `termination_date`: Date the driver left (Null if still active).
*   `license_number`: CDL Identifier.
*   `license_state`: Issuing state of CDL.
*   `date_of_birth`: Driver's age demographic.
*   `home_terminal`: The primary facility for the driver.
*   `employment_status`: Current HR status ('Active', 'Terminated').
*   `cdl_class`: License classification ('A', 'B', etc.).
*   `years_experience`: Derived metric of driving tenure.
*   `is_active_driver`: Boolean engineered feature checking if `termination_date` is Null.

---

### 3. `clean_fuel_purchases.csv`
**Total Columns:** 11
**Details:** Tracks all over-the-road fuel expenditures for the fleet.
*   `fuel_purchase_id`: Primary key for the transaction.
*   `trip_id`: Links the expense to a specific route.
*   `truck_id`: Links to the asset (or 'UNKNOWN_TRUCK' for ghost dimensions).
*   `driver_id`: Links to the purchaser (or 'UNKNOWN_DRIVER').
*   `purchase_date`: Cleaned timestamp of the swipe.
*   `location_city`: City of the fuel stop.
*   `location_state`: State of the fuel stop.
*   `gallons`: Quantity of fuel poured.
*   `price_per_gallon`: Spot rate of the fuel.
*   `total_cost`: `gallons` * `price_per_gallon`.
*   `fuel_card_number`: Encrypted/truncated card reference.

---

### 4. `clean_maintenance_records.csv`
**Total Columns:** 13
**Details:** NLP-Processed logs detailing truck repairs and preventative upkeep.
*   `maintenance_id`: Repair event ID.
*   `truck_id`: The asset that received service.
*   `maintenance_date`: Date service was performed.
*   `maintenance_type`: Categorical tracking of PM vs Unplanned repair.
*   `odometer_reading`: Asset mileage at time of service.
*   `labor_hours`: Time spent by mechanics.
*   `labor_cost`: Financial tracking of internal/external mechanic labor.
*   `parts_cost`: Expenses for replaced parts.
*   `total_cost`: Absolute financial liability of the repair event.
*   `facility_location`: Where repair happened.
*   `downtime_hours`: Amount of time the asset was off the road.
*   `service_description`: Original raw free-text notes from the mechanic.
*   `Delay_Reason`: Our engineered NLP attribute standardizing the text into analytics categories (e.g., 'Mechanical Failure').

---

### 5. `clean_safety_incidents.csv`
**Total Columns:** 16
**Details:** Processed logs containing accidents, fines, or bodily injuries.
*   `incident_id`: Unique tracking key.
*   `trip_id`: Contextual link to the route in progress.
*   `truck_id`: The asset involved.
*   `driver_id`: Operations staff involved.
*   `incident_date`: When the event occurred.
*   `incident_type`: Type of event (Crash, Ticket, etc.).
*   `location_city`: Where it occurred.
*   `location_state`: State jurisdiction.
*   `at_fault_flag`: Boolean (1/0) tracking liability.
*   `injury_flag`: Boolean tracking bodily harm.
*   `vehicle_damage_cost`: Fleet asset loss amount.
*   `cargo_damage_cost`: Customer freight loss amount.
*   `claim_amount`: Insurance exposure.
*   `preventable_flag`: DOT Preventability ruling.
*   `description`: Unstructured officer/safety notes.
*   `Incident_Category`: NLP-extracted analytical tag.

---

### 6. `clean_trips.csv`
**Total Columns:** 12
**Details:** Represents the physical journey of a truck dispatch.
*   `trip_id`: Core identifier of the dispatch.
*   `load_id`: The cargo assignment on the truck.
*   `driver_id`: Who drove it.
*   `truck_id`: The tractor used.
*   `trailer_id`: The trailer pulled.
*   `dispatch_date`: Day the trip began.
*   `actual_distance_miles`: Total driven mileage.
*   `actual_duration_hours`: ELD time consumed.
*   `fuel_gallons_used`: Engine consumption.
*   `average_mpg`: Efficiency of the dispatch.
*   `idle_time_hours`: Wasted fuel time metric.
*   `trip_status`: Operational position state ('Completed', 'In-Transit').

---

### 7. `customers.csv`
**Total Columns:** 8
**Details:** B2B Clients utilizing the logistics service.
*   `customer_id`: Primary Key.
*   `customer_name`: Business entity name.
*   `customer_type`: Retail, Manufacturing, etc.
*   `credit_terms_days`: Net terms limits.
*   `primary_freight_type`: Typical cargo (Dry Van, Reefer, Flatbed).
*   `account_status`: Active/Suspended.
*   `contract_start_date`: Tenure calculation baseline.
*   `annual_revenue_potential`: Financial ceiling estimate.

---

### 8. `driver_monthly_metrics.csv`
**Total Columns:** 9
**Details:** Pre-aggregated monthly analytical snapshot of Driver performance.
*   `driver_id`: Identifies the employee.
*   `month`: The aggregated month span.
*   `trips_completed`: Total successfully delivered loads.
*   `total_miles`: Sum of distance driven.
*   `total_revenue`: Freight revenue billed by that driver.
*   `average_mpg`: Efficiency average.
*   `total_fuel_gallons`: Environmental cost total.
*   `on_time_delivery_rate`: Percentage of SLA conformance.
*   `average_idle_hours`: Waste tracking per driver.

---

### 9. `facilities.csv`
**Total Columns:** 9
**Details:** Locations and terminals associated with freight movement.
*   `facility_id`: Reference Key.
*   `facility_name`: The building identifier.
*   `facility_type`: Cross-dock, Warehouse, Terminal, etc.
*   `city`: Geographical location.
*   `state`: State location.
*   `latitude`: Map coordinate for geospatial visuals.
*   `longitude`: Map coordinate.
*   `dock_doors`: Facility capacity.
*   `operating_hours`: Availability window.

---

### 10. `loads.csv`
**Total Columns:** 12
**Details:** The financial freight contract assignment (The "What" being moved).
*   `load_id`: Unique freight record.
*   `customer_id`: Payer for the freight.
*   `route_id`: The geographical path assigned.
*   `load_date`: When it was tendered.
*   `load_type`: Contract vs Spot market.
*   `weight_lbs`: Cargo mass.
*   `pieces`: Pallet count.
*   `revenue`: Top-line income from the load.
*   `fuel_surcharge`: Pass-through fuel billing.
*   `accessorial_charges`: Extra bills (detention, lumper).
*   `load_status`: Current lifecycle.
*   `booking_type`: EDI vs Manual tender.

---

### 11. `routes.csv`
**Total Columns:** 9
**Details:** Standardized paths/lanes traveled by the fleet.
*   `route_id`: Lane identifier.
*   `origin_city`: Beginning point of lane.
*   `origin_state`: Origin state.
*   `destination_city`: End point of lane.
*   `destination_state`: Destination state.
*   `typical_distance_miles`: Baseline mileage expectation.
*   `base_rate_per_mile`: Baseline contractual pricing.
*   `fuel_surcharge_rate`: Index reference.
*   `typical_transit_days`: SLA baseline.

---

### 12. `trailers.csv`
**Total Columns:** 9
**Details:** Unmotorized capacity assets.
*   `trailer_id`: PK.
*   `trailer_number`: Stenciled painted ID.
*   `trailer_type`: Dry Van, Reefer.
*   `length_feet`: Capacity string (53', 48').
*   `model_year`: Age tracker.
*   `vin`: Legal identifier.
*   `acquisition_date`: Purchase tracking.
*   `status`: Ready vs Out-of-service.
*   `current_location`: Yard tracking link.

---

### 13. `truck_utilization_metrics.csv`
**Total Columns:** 10
**Details:** Pre-aggregated monthly snapshot of Tractor performance.
*   `truck_id`: The tractor asset.
*   `month`: The analysis month.
*   `trips_completed`: Dispatch count.
*   `total_miles`: Distance worn on engine.
*   `total_revenue`: Freight billed during tractor's watch.
*   `average_mpg`: Engine efficiency.
*   `maintenance_events`: PMs or issues in the month.
*   `maintenance_cost`: Financial tracking of those issues.
*   `downtime_hours`: Lost gross revenue time.
*   `utilization_rate`: Percent of time the truck was actively moving freight over the month.

---

### 14. `trucks.csv`
**Total Columns:** 11
**Details:** Motorized power units of the fleet.
*   `truck_id`: PK.
*   `unit_number`: Painted ID.
*   `make`: Freightliner, Peterbilt, etc.
*   `model_year`: Vintage.
*   `vin`: Vehicle Identification Number.
*   `acquisition_date`: Asset age ledger.
*   `acquisition_mileage`: Purchase state.
*   `fuel_type`: Diesel vs EV.
*   `tank_capacity_gallons`: Range capability.
*   `status`: Dispatchable or in repair.
*   `home_terminal`: Domicile location.
