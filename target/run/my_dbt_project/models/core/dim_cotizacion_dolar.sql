
      
  
    

  create  table
    `core`.`dim_cotizacion_dolar`
    
    
      as
    
    (
      

select 
    fecha,
    cotizacion_en_pesos
from `staging`.`stg_cotizaciones`

    )

  
  