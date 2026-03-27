with

source as (
    select * from {{ source('fred', 't10y2y') }}
),

final as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as spread_10y_2y
    from source
    where date  is not null
      and value is not null
      and value != '.'
)

select * from final
