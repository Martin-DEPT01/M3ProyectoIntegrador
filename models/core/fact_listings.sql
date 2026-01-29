{{ config(unique_key='listing_id') }}

select
    listing_id,
    host_id,
    host_name,
    price,
    minimum_nights,
    number_of_reviews,
    last_review,
    availability_365,
    neighbourhood, -- Llave para unir con dim_neighbourhoods
    room_type      -- Llave para unir con dim_room_types
from {{ ref('stg_listings') }}
