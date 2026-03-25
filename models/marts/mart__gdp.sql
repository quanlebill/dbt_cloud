{{config(materialized='table')}}
with

time_spine as (
    select * from {{'mart__standard_year'}}
),
source as (
    select * from {{ref('pp_snowflake__gdp')}}
),

final as (
    select
        *
    from source
)

select * from final