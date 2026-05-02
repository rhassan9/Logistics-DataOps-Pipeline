-- stg_customers.sql
-- Staging model for customers.csv

with source as (
    select * from {{ source('logistics_raw', 'customers') }}
),

renamed as (
    select
        cast(customer_id               as varchar)     as customer_id,
        cast(customer_name             as varchar)     as customer_name,
        cast(customer_type             as varchar)     as customer_type,
        cast(credit_terms_days         as integer)     as credit_terms_days,
        cast(primary_freight_type      as varchar)     as primary_freight_type,
        cast(account_status            as varchar)     as account_status,
        cast(contract_start_date       as date)        as contract_start_date,
        cast(annual_revenue_potential  as numeric(14,2)) as annual_revenue_potential
    from source
)

select * from renamed
