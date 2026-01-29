{{ config(
    materialized='incremental',
    unique_key='fecha',      
) }}

with source_data as (

    select *
    from {{ source('raw', 'mysql_currency_raw') }}

),

casted as (

    select
        -- En MySQL se usa CAST. Si falla, devuelve NULL o error según el modo.
        cast(fecha as date) as fecha,
        cast(cotizacion_en_pesos as decimal(10,2)) as cotizacion_en_pesos
    from source_data

),

filtered as (

    select *
    from casted
    where fecha is not null
      and cotizacion_en_pesos is not null

)

select *
from filtered
{% if is_incremental() %}
-- El filtro DEBE estar dentro de la consulta final para que dbt lo reconozca
where fecha not in (select fecha from {{ this }})
{% endif %}
