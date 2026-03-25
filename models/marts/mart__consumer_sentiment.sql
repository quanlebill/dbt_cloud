{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('pp_fred__consumer_sentiment') }}
),

final as (
    select
        cast(observation_date as varchar)                             as sentiment_pii,
        country_name,
        cast(date_trunc('month', observation_date) as timestamp_ntz) as observation_date,
        consumer_sentiment
    from source
)

select * from final
order by observation_date desc
