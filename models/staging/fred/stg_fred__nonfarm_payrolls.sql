with

source as (
    select * from {{ source('fred', 'payems') }}
),

raw as (
    select
        date_trunc('month', cast(date as date)) as observation_date,
        cast(value as float)                    as payrolls_level
    from source
    where date  is not null
      and value is not null
      and value != '.'
      and cast(value as float) > 0
),

final as (
    select
        observation_date,
        avg(payrolls_level) as payrolls_level
    from raw
    group by observation_date
)

select * from final
