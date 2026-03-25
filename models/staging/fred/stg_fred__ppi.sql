with

source as (
    select * from {{ source('fred', 'ppiaco') }}
),

raw as (
    select
        lower(country)                          as country_name,
        date_trunc('month', cast(date as date)) as observation_date,
        cast(value as float)                    as ppi_level
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
        avg(ppi_level) as ppi_level
    from raw
    group by country_name, observation_date
)

select * from final
