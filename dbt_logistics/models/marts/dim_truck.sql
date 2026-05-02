-- dim_truck.sql
-- Final Truck dimension with surrogate key and calculated columns.
-- Calculated: truck_age_years, is_underutilized_asset

with trucks as (
    select * from {{ ref('stg_trucks') }}
),

-- Sum total lifetime miles per truck from the trips staging model
lifetime_miles as (
    select
        truck_id,
        sum(actual_distance_miles) as total_lifetime_miles
    from {{ ref('stg_trips') }}
    group by truck_id
),

-- Max date from the dataset to anchor age and utilization logic
max_date as (
    select max(dispatch_date) as dataset_max_date
    from {{ ref('stg_trips') }}
)

select
    -- Surrogate Key
    row_number() over (order by t.truck_id) as truck_sk,

    -- Natural Key (includes 'UNKNOWN_TRUCK' Ghost Dimension values)
    t.truck_id,

    -- Descriptive attributes
    t.unit_number,
    t.make,
    t.model_year,
    t.vin,
    t.acquisition_date,
    t.acquisition_mileage,
    t.fuel_type,
    t.tank_capacity_gallons,
    t.status,
    t.home_terminal,

    -- CALCULATED: Truck age in years, using max dataset date (dataset is 2024)
    extract(year from m.dataset_max_date) - t.model_year
        as truck_age_years,

    -- CALCULATED: Flag underutilized assets:
    -- Active truck, owned 30+ days, but driven fewer than 2,000 total miles
    case
        when t.status = 'Ready'
         and (m.dataset_max_date - t.acquisition_date) > 30
         and coalesce(lm.total_lifetime_miles, 0) < 2000
        then 1
        else 0
    end as is_underutilized_asset

from trucks t
cross join max_date m
left join lifetime_miles lm on t.truck_id = lm.truck_id
