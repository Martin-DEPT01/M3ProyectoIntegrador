
  
    

  create  table
    `core`.`fact_listings__dbt_tmp`
    
    
      as
    
    (
      

with obt_data as (
    select * from `staging`.`stg_listings` 
), 

dimension_neighbourhoods as (
    select * from `core`.`dim_neighbourhoods`
),

dimension_room_type as (
    select * from `core`.`dim_room_types`
)

select 
    obt.listing_id,
    obt.host_id,
    obt.host_name,
    obt.price,
    obt.minimum_nights,
    obt.number_of_reviews,
    obt.last_review,
    obt.availability_365,
    dn.id_neighbourhood, -- Llave para unir con dim_neighbourhoods
    drt.id_room_type      -- Llave para unir con dim_room_types
from obt_data obt
left join dimension_neighbourhoods dn 
on obt.neighbourhood = dn.neighbourhood and obt.neighbourhood_group = dn.neighbourhood_group
left join dimension_room_type drt
on obt.room_type = drt.room_type
    )

  