with 

source as (
    select * from {{source('snowflake', 'imf_nominal_gdp')}}
),

final as (
    select
        country,
        type ,
        year,
        population_change_pct,
        gdp_nominal_usd,
        gdp_per_capita_usd,
        gdp_growth_pct
    from source
)

select * from final
