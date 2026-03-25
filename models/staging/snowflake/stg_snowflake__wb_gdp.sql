with
source as (
    select * from {{source('snowflake', 'wb_gdp')}}
),

raw as (
    select 
        LOWER(country) as country_name,
        LOWER(type) as type,
        cast(year as int) as order_year,
        gdp_nominal_usd as gdp_nominal,
        gdp_real_usd as gdp_real,
        population_change_pct as population_change
    from source
    where country is not null
    and type is not null
    and LOWER(type) in ('history', 'projection')
    and year is not null
    and year > 0
    and gdp_nominal_usd > 0
    and gdp_real_usd > 0
    order by year
),

--making sure no duplicate in combination (country_name, type, year)
final as (
    select
        country_name,
        type,
        order_year,
        AVG(gdp_nominal) as gdp_nominal,
        AVG(gdp_real) as gdp_real,
        AVG(population_change) as population_change

    from raw
    group by country_name, type, order_year
)

select * from final
