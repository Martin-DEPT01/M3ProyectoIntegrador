select
      count(*) as failures,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_warn,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_error
    from (
      
    
    

with child as (
    select id_room_type as from_field
    from `core`.`fact_listings`
    where id_room_type is not null
),

parent as (
    select id_room_type as to_field
    from `core`.`dim_room_types`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test