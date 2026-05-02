-- dim_driver.sql
-- Final Driver dimension with surrogate key and calculated columns.
-- Calculated: driver_tenure_days, driver_safety_risk_index

with drivers as (
    select * from {{ ref('stg_drivers') }}
),

-- Count at-fault incidents per driver from the safety staging model
at_fault_counts as (
    select
        driver_id,
        count(*) as at_fault_incidents
    from {{ ref('stg_safety_incidents') }}
    where at_fault_flag = 1
    group by driver_id
),

-- Get the dataset max date to use as a reference for tenure calculations
max_date as (
    select max(dispatch_date) as dataset_max_date
    from {{ ref('stg_trips') }}
)

select
    -- Surrogate Key
    row_number() over (order by d.driver_id) as driver_sk,

    -- Natural Key
    d.driver_id,

    -- Descriptive attributes
    d.first_name,
    d.last_name,
    d.hire_date,
    d.termination_date,
    d.license_number,
    d.license_state,
    d.date_of_birth,
    d.home_terminal,
    d.employment_status,
    d.cdl_class,
    d.years_experience,
    d.is_active_driver,

    -- CALCULATED: Total days employed, anchored to max dataset date (not today)
    coalesce(d.termination_date, m.dataset_max_date) - d.hire_date
        as driver_tenure_days,

    -- CALCULATED: At-fault incidents per year of tenure (Safety Risk Index)
    -- Uses at_fault_flag from stg_safety_incidents, divided by tenure in years
    round(
        coalesce(af.at_fault_incidents, 0)::numeric
        / nullif(
            (coalesce(d.termination_date, m.dataset_max_date) - d.hire_date) / 365.0,
            0
        ),
        4
    ) as driver_safety_risk_index

from drivers d
cross join max_date m
left join at_fault_counts af on d.driver_id = af.driver_id
