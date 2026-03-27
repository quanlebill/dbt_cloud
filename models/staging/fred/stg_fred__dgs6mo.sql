with

source as (
    select * from {{ source('fred', 'dgs6mo') }}
),

final as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as yield_6mo
    from source
    where date  is not null
      and value is not null
      and value != '.'
)

select * from final
