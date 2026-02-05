
    
    

select
    fecha as unique_field,
    count(*) as n_records

from `core`.`dim_cotizacion_dolar`
where fecha is not null
group by fecha
having count(*) > 1


