with

source as (
    select * from {{ source('fred', 'fedfunds') }}
),

raw as (
    select
        date_trunc('month', cast(date as date)) as observation_date,
        cast(value as float)                    as fed_funds_rate
    from source
    where date  is not null
      and value is not null
      and value != '.'
      and cast(value as float) >= 0
),

final as (
    select
        observation_date,
        avg(fed_funds_rate) as fed_funds_rate
    from raw
    group by observation_date
)

select * from final
