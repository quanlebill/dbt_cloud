with

source as (
    select * from {{ source('fred', 'umcsent') }}
),

raw as (
    select
        lower(country)                          as country_name,
        date_trunc('month', cast(date as date)) as observation_date,
        cast(value as float)                    as consumer_sentiment
    from source
    where date  is not null
      and value is not null
      and value != '.'
      and cast(value as float) > 0
),

final as (
    select
        country_name,
        observation_date,
        avg(consumer_sentiment) as consumer_sentiment
    from raw
    group by country_name, observation_date
)

select * from final
