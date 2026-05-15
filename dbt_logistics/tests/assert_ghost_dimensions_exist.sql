-- Fails if the Ghost dimensions are missing
with driver_ghost as (
    select count(*) as cnt from {{ ref('dim_driver') }} where driver_id = 'UNKNOWN_DRIVER'
),
truck_ghost as (
    select count(*) as cnt from {{ ref('dim_truck') }} where truck_id = 'UNKNOWN_TRUCK'
)
select * from driver_ghost where cnt = 0
union all
select * from truck_ghost where cnt = 0
