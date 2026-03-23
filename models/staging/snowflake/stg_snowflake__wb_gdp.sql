with 
source as (
    select * from {{source('snowflake', 'wb_gdp')}}
),

final as (
    select 
        country,
        type,
        year,
        gdp_nominal_usd,
        gdp_real_usd,
        population_change_pct
    from source
)

select * from final
