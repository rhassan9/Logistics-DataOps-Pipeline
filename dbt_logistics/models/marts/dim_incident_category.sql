-- dim_incident_category.sql
-- Conformed Dimension mapping NLP categories and attributes from Safety sources.
-- Grain: 1 row = 1 unique combination of attributes.

with default_causes as (
    -- Hardcoded default for on-time shipments
    select
        'No Delay'              as incident_category,
        'None'                  as incident_type,
        0                       as at_fault_flag,
        0                       as injury_flag,
        0                       as is_preventable
    union all
    -- Hardcoded default for unexplained late shipments
    select
        'Unknown / Unreported Delay' as incident_category,
        'Unknown'               as incident_type,
        0                       as at_fault_flag,
        0                       as injury_flag,
        0                       as is_preventable
),

safety_causes as (
    select
        incident_category,
        incident_type,
        cast(max(at_fault_flag) as integer) as at_fault_flag,
        cast(max(injury_flag) as integer) as injury_flag,
        cast(max(preventable_flag) as integer) as is_preventable
    from {{ ref('stg_safety_incidents') }}
    where incident_category is not null
    group by incident_category, incident_type
),

combined as (
    select * from default_causes
    union all
    select * from safety_causes
)

select
    row_number() over (order by incident_category, incident_type) as incident_category_sk,
    incident_category,
    incident_type,
    at_fault_flag,
    injury_flag,
    is_preventable
from combined
