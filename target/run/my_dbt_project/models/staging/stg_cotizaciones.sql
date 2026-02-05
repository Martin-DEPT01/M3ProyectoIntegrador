
  create view `staging`.`stg_cotizaciones__dbt_tmp`
    
    
  as (
    -- models/stg_cotizacion.sql

with source as (
    select * from `raw`.`mysql_currency_raw`
),

casted as (

    select
        -- En MySQL se usa CAST. Si falla, devuelve NULL o error según el modo.
        cast(fecha as date) as fecha,
        cast(cotizacion_en_pesos as decimal(10,2)) as cotizacion_en_pesos
    from source

),

filtered as (

    select *
    from casted
    where fecha is not null
      and cotizacion_en_pesos is not null

)

select * from filtered
  );