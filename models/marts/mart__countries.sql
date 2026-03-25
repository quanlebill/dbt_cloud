{{config(materialized='table')}}

with

source as (
    select * from {{ref('pp_snowflake__countries')}}
),

final as (select * from source)

select * from final