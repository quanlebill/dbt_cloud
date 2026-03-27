with

source as (
    select * from {{ source('fred', 'dfedtarl') }}
),

final as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as target_lower
    from source
    where date  is not null
      and value is not null
      and value != '.'
      and cast(value as float) >= 0
)

select * from final
