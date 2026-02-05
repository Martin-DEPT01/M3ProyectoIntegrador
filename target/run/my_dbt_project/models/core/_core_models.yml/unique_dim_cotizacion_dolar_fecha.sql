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
    fecha as unique_field,
    count(*) as n_records

from `core`.`dim_cotizacion_dolar`
where fecha is not null
group by fecha
having count(*) > 1



      
    ) dbt_internal_test