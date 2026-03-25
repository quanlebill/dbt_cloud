{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('pp_fred__nonfarm_payrolls') }}
),

final as (
    select
        cast(observation_date as varchar)                             as nonfarm_pii,
        cast(date_trunc('month', observation_date) as timestamp_ntz) as observation_date,
        payrolls_level
    from source
)

select * from final
order by observation_date desc
