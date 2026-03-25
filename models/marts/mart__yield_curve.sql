{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('pp_fred__yield_curve') }}
),

final as (
    select
        cast(observation_date as varchar)       as yield_curve_pii,
        cast(observation_date as timestamp_ntz) as observation_date,
        y1m,
        y3m,
        y6m,
        y1,
        y2,
        y5,
        y10,
        y30,
        spread_10y_2y,
        spread_10y_3m
    from source
)

select * from final
order by observation_date desc
