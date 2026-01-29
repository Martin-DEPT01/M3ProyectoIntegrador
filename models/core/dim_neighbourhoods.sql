{{ config(unique_key='neighbourhood') }}

select distinct
    neighbourhood,
    neighbourhood_group
from {{ ref('stg_listings') }}
