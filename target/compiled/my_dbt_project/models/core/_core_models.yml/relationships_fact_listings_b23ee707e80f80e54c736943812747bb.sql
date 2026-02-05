
    
    

with child as (
    select id_neighbourhood as from_field
    from `core`.`fact_listings`
    where id_neighbourhood is not null
),

parent as (
    select id_neighbourhood as to_field
    from `core`.`dim_neighbourhoods`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


