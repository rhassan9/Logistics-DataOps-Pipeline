-- dim_customer.sql
-- Final Customer dimension with surrogate key and calculated column.
-- Calculated: concentration_risk_tier

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- Calculate each customer's total billed revenue from completed loads
customer_revenue as (
    select
        l.customer_id,
        sum(l.revenue + l.fuel_surcharge + l.accessorial_charges) as customer_total_revenue
    from {{ ref('stg_loads') }} l
    group by l.customer_id
),

-- Calculate the total company-wide billed revenue for the ratio
company_total as (
    select sum(revenue + fuel_surcharge + accessorial_charges) as company_total_revenue
    from {{ ref('stg_loads') }}
)

select
    -- Surrogate Key
    row_number() over (order by c.customer_id) as customer_sk,

    -- Natural Key
    c.customer_id,

    -- Descriptive attributes
    c.customer_name,
    c.customer_type,
    c.credit_terms_days,
    c.primary_freight_type,
    c.account_status,
    c.contract_start_date,
    c.annual_revenue_potential,

    -- CALCULATED: Concentration Risk Tier
    -- If a single customer represents >15% of total revenue, flag as High Risk
    case
        when cr.customer_total_revenue / nullif(ct.company_total_revenue, 0) > 0.15
        then 'High Risk (>15%)'
        else 'Normal'
    end as concentration_risk_tier

from customers c
cross join company_total ct
left join customer_revenue cr on c.customer_id = cr.customer_id
