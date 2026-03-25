-- Joins DFEDTARL (lower) and DFEDTARU (upper) into a single daily target range table
with

lower_src as (
    select * from {{ source('fred', 'dfedtarl') }}
),

upper_src as (
    select * from {{ source('fred', 'dfedtaru') }}
),

lower_raw as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as target_lower
    from lower_src
    where date  is not null
      and value is not null
      and value != '.'
      and cast(value as float) >= 0
),

upper_raw as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as target_upper
    from upper_src
    where date  is not null
      and value is not null
      and value != '.'
      and cast(value as float) >= 0
),

final as (
    select
        coalesce(l.country_name, u.country_name)         as country_name,
        coalesce(l.observation_date, u.observation_date) as observation_date,
        l.target_lower,
        u.target_upper
    from lower_raw l
    full join upper_raw u on l.observation_date = u.observation_date
)

select * from final
