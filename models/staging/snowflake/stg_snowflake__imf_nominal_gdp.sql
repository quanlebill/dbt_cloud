with

source as (
    select * from {{source('snowflake', 'imf_nominal_gdp')}}
),

raw as (
    select
        country as country_name,
        LOWER(type) as type,
        cast(year as int) as year,
        population_change_pct as population_change,
        gdp_nominal_usd as gdp_nominal,
        gdp_per_capita_usd as gdp_per_capita,
        gdp_growth_pct as gdp_growth
    from source
    where country is not null
    and type is not null
    and LOWER(type) in ('history', 'projection')
    and year is not null
    and year > 0
    and gdp_nominal_usd > 0
    and gdp_per_capita_usd > 0
),

final as (
    select
        country_name,
        type,
        year,
        AVG(population_change) as population_change,
        AVG(gdp_nominal) as gdp_nominal,
        AVG(gdp_per_capita) as gdp_per_capita,
        AVG(gdp_growth) as gdp_growth
    from raw
    group by country_name, type, year
)

select * from final
