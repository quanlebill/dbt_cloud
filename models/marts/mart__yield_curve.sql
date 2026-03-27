{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('tr_fred__yield_curve') }}
),

final as (
    select
        cast(observation_date as varchar)       as yield_curve_pii,
        country_name,
        cast(observation_date as timestamp_ntz) as observation_date,
        yield_1mo,
        yield_3mo,
        yield_6mo,
        yield_1y,
        yield_2y,
        yield_5y,
        yield_10y,
        yield_30y,
        spread_10y_2y,
        spread_10y_3m
    from source
)

select * from final
order by observation_date desc
