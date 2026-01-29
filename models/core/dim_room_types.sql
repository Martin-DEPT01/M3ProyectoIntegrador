{{ config(unique_key='room_type') }}

select distinct
    room_type
from {{ ref('stg_listings') }}
