{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('pp_fred__fed_funds_monthly') }}
),

final as (
    select
        cast(observation_date as varchar)                             as fed_funds_monthly_pii,
        cast(date_trunc('month', observation_date) as timestamp_ntz) as observation_date,
        fed_funds_rate
    from source
)

select * from final
order by observation_date desc
