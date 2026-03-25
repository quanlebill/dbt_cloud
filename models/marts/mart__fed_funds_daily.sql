{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('pp_fred__fed_funds_daily') }}
),

final as (
    select
        cast(observation_date as varchar)          as fed_funds_daily_pii,
        cast(observation_date as timestamp_ntz)    as observation_date,
        fed_funds_rate,
        target_lower,
        target_upper
    from source
)

select * from final
order by observation_date desc
