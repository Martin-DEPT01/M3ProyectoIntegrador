-- models/stg_listings.sql
with source as (
    select * from {{ source('raw_data', 'mysql_csv_raw') }}
),

casted as (
    select
        cast(id as unsigned) as listing_id,
        name,
        cast(host_id as unsigned) as host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        cast(latitude as decimal(10,8)) as latitude,
        cast(longitude as decimal(11,8)) as longitude,
        room_type,
        cast(price as decimal(10,2)) as price,
        cast(minimum_nights as unsigned) as minimum_nights,
        cast(number_of_reviews as unsigned) as number_of_reviews,
        cast(last_review as date) as last_review,
        cast(reviews_per_month as decimal(10,2)) as reviews_per_month,
        cast(availability_365 as unsigned) as availability_365
    from source
    where last_review is not null 
      and last_review != '' -- Filtro extra por si vienen strings vacíos
)

select * from casted
