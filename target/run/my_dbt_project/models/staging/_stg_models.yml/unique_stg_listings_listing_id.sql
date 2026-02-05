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
      
    
    

select
    listing_id as unique_field,
    count(*) as n_records

from `staging`.`stg_listings`
where listing_id is not null
group by listing_id
having count(*) > 1



      
    ) dbt_internal_test