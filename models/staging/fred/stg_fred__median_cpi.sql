with

source as (
    select * from {{ source('fred', 'medcpim159sfrbcle') }}
),

raw as (
    select
        lower(country)                          as country_name,
        date_trunc('month', cast(date as date)) as observation_date,
        cast(value as float)                    as median_cpi_yoy
    from source
    where date  is not null
      and value is not null
      and value != '.'
),

final as (
    select
        country_name,
        observation_date,
        avg(median_cpi_yoy) as median_cpi_yoy
    from raw
    group by country_name, observation_date
)

select * from final
