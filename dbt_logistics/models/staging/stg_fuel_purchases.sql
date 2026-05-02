-- stg_fuel_purchases.sql
-- Staging model for clean_fuel_purchases.csv

with source as (
    select * from {{ source('logistics_raw', 'clean_fuel_purchases') }}
),

renamed as (
    select
        cast(fuel_purchase_id  as varchar)     as fuel_purchase_id,
        cast(trip_id           as varchar)     as trip_id,
        cast(truck_id          as varchar)     as truck_id,
        cast(driver_id         as varchar)     as driver_id,
        cast(purchase_date     as date)        as purchase_date,
        cast(location_city     as varchar)     as location_city,
        cast(location_state    as varchar)     as location_state,
        cast(gallons           as numeric(10,3)) as gallons,
        cast(price_per_gallon  as numeric(8,4)) as price_per_gallon,
        cast(total_cost        as numeric(12,2)) as total_cost,
        cast(fuel_card_number  as varchar)     as fuel_card_number
    from source
)

select * from renamed
