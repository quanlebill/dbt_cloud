with

source as (
    select * from {{ source('snowflake', 'countries') }}
),

raw as (
    select
        LOWER(name) as country_name,
        LOWER(iso_alpha_2_code) as iso_alpha_2_code,
        LOWER(iso_alpha_3_code) as iso_alpha_3_code
    from source
    where name is not null
),

final as (
    select * from raw
)

select * from final