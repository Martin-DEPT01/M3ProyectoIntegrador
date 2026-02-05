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
      
    
    



select cotizacion_en_pesos
from `core`.`dim_cotizacion_dolar`
where cotizacion_en_pesos is null



      
    ) dbt_internal_test