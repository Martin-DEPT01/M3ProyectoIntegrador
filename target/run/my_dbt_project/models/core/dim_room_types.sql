
  
    

  create  table
    `core`.`dim_room_types__dbt_tmp`
    
    
      as
    
    (
      

with distinct_room_type as (
select distinct
    room_type
from `staging`.`stg_listings`
)

select
    row_number() over (order by room_type) as id_room_type,
    room_type
from distinct_room_type
    )

  