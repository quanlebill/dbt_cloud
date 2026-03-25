-- Joins T10Y2Y and T10Y3M into a single daily yield curve spreads table
with

t10y2y_src as (select * from {{ source('fred', 't10y2y') }}),
t10y3m_src as (select * from {{ source('fred', 't10y3m') }}),

spread_10y2y as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as spread_10y_2y
    from t10y2y_src
    where value is not null and value != '.'
),

spread_10y3m as (
    select
        lower(country)       as country_name,
        cast(date as date)   as observation_date,
        cast(value as float) as spread_10y_3m
    from t10y3m_src
    where value is not null and value != '.'
),

final as (
    select
        coalesce(a.country_name, b.country_name)         as country_name,
        coalesce(a.observation_date, b.observation_date) as observation_date,
        a.spread_10y_2y,
        b.spread_10y_3m
    from spread_10y2y a
    full join spread_10y3m b on a.observation_date = b.observation_date
    where coalesce(a.observation_date, b.observation_date) is not null
)

select * from final
