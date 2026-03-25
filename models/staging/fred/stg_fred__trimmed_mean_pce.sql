with

source as (
    select * from {{ source('fred', 'pcetrim12m159sfrbdal') }}
),

raw as (
    select
        date_trunc('month', cast(date as date)) as observation_date,
        cast(value as float)                    as trimmed_mean_pce_yoy
    from source
    where date  is not null
      and value is not null
      and value != '.'
),

final as (
    select
        observation_date,
        avg(trimmed_mean_pce_yoy) as trimmed_mean_pce_yoy
    from raw
    group by observation_date
)

select * from final
