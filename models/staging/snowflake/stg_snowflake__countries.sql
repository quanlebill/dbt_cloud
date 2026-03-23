{{ config(materialized='view') }}
with

source as (
    select * from {{ source('snowflake', 'countries') }}
),

raw as (
    select
        name as country_name,
        iso_alpha_2_code,
        iso_alpha_3_code
    from source
    where name is not null
)

final as (
    select * from raw
)

select * from final