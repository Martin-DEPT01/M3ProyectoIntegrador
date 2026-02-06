{{ config(
    materialized='incremental',
    unique_key='fecha',      
) }}

select 
    fecha,
    cotizacion_en_pesos
from {{ ref('stg_cotizaciones') }}
{% if is_incremental() %}
-- El filtro DEBE estar dentro de la consulta final para que dbt lo reconozca
where fecha not in (select fecha from {{ this }})
{% endif %}
