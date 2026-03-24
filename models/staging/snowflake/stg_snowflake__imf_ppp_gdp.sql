with

source as (
    select * from {{source('snowflake', 'imf_ppp_gdp')}}
),

raw as (
    select
        LOWER(country) as country_name,
        LOWER(type) as type,
        cast(year as int) as year,
        population_change_pct as population_change,
        gdp_ppp_usd as gdp_ppp,
        gdp_ppp_per_capita_usd as gdp_ppp_per_capita,
        gdp_ppp_growth_pct as gdp_ppp_growth
    from source
    where country is not null
    and type is not null
    and LOWER(type) in ('history', 'projection')
    and year is not null
    and year > 0
    and gdp_ppp_usd > 0
    and gdp_ppp_per_capita_usd > 0
    order by year
),

--making sure no duplicate in combination (country_name, type, year)
final as (
    select
        country_name,
        type,
        year,
        AVG(population_change) as population_change,
        AVG(gdp_ppp) as gdp_ppp,
        AVG(gdp_ppp_per_capita) as gdp_ppp_per_capita,
        AVG(gdp_ppp_growth) as gdp_ppp_growth
    from raw
    group by country_name, type, year
)


select * from final