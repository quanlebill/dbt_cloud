with 

source as (
    select * from {{source('snowflake', 'imf_ppp_gdp')}}
),

final as (
    select
        country,
        type,
        year,
        population_change_pct,
        gdp_ppp_usd,
        gdp_ppp_per_capita_usd,
        gdp_ppp_growth_pct
    from source
)

select * from final