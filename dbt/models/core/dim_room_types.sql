{{ config(unique_key='room_type') }}

with distinct_room_type as (
select distinct
    room_type
from {{ ref('stg_listings') }}
)

select
    row_number() over (order by room_type) as id_room_type,
    room_type
from distinct_room_type