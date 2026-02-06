{{ config(unique_key='neighbourhood') }}

with distinct_neighbourhood as (
select distinct
    neighbourhood,
    neighbourhood_group
from {{ ref('stg_listings') }}
)

select
    row_number() over (order by neighbourhood, neighbourhood_group) as id_neighbourhood,
    neighbourhood,
    neighbourhood_group
from distinct_neighbourhood