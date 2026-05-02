-- dim_date.sql
-- Standard auto-generated Date dimension.
-- Generates one row per calendar day covering the entire dataset range (2022-2025).

with date_spine as (
    -- Generate a series of dates.
    select generate_series(
        '2022-01-01'::date,
        '2024-12-31'::date,
        '1 day'::interval
    )::date as full_date
)

select
    -- Integer surrogate key: YYYYMMDD format, e.g., 20240115
    cast(to_char(full_date, 'YYYYMMDD') as integer) as date_sk,

    full_date,
    to_char(full_date, 'Day')   as day_of_week,
    to_char(full_date, 'Month') as month_name,
    extract(month from full_date)::integer  as month_number,
    extract(quarter from full_date)::integer as quarter,
    extract(year from full_date)::integer   as year,

    -- Flag weekends for operational reporting
    case when extract(dow from full_date) in (0, 6) then 1 else 0 end as is_weekend

from date_spine
order by full_date
