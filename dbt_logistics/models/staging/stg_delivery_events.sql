-- stg_delivery_events.sql
-- Staging model for clean_delivery_events.csv
-- Used to compute delay_duration_minutes and is_sla_breached in Fact_Shipment.

with source as (
    select * from {{ source('logistics_raw', 'clean_delivery_events') }}
),

renamed as (
    select
        cast(event_id            as varchar)    as event_id,
        cast(load_id             as varchar)    as load_id,
        cast(trip_id             as varchar)    as trip_id,
        cast(event_type          as varchar)    as event_type,
        cast(facility_id         as varchar)    as facility_id,
        cast(scheduled_datetime  as timestamp)  as scheduled_datetime,
        cast(actual_datetime     as timestamp)  as actual_datetime,
        cast(detention_minutes   as numeric(10,2)) as detention_minutes,
        cast(on_time_flag        as smallint)   as on_time_flag,
        cast(is_telematics_drop  as smallint)   as is_telematics_drop
    from source
)

select * from renamed
