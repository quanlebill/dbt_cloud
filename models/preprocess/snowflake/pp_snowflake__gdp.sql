with
nominal_gdp_src as (
    select * from {{ref('stg_snowflake__imf_nominal_gdp')}}
),
ppp_gdp_src as (
    select * from {{ref('stg_snowflake__imf_ppp_gdp')}}
),
wb_gdp_src as (
    select * from {{ref('stg_snowflake__wb_gdp')}}
),
countries_src as (
    select * from {{ref('pp_snowflake__countries')}}
),

stage as (
    select
        src1.country_name as country_name,
        src1.type as type,
        src1.year as year,
        src1.gdp_nominal as gdp_nominal,
        src1.gdp_nominal_per_capita as gdp_nominal_per_capita ,
        src2.gdp_ppp as gdp_ppp,
        src2.gdp_ppp_per_capita as gdp_ppp_per_capita,
        src3.gdp_real as gdp_real,
        src1.population_change as population_change
    from nominal_gdp_src as src1
    join ppp_gdp_src as src2
    on src1.country_name = src2.country_name
    and src1.type = src2.type
    and src1.year = src2.year
    full join wb_gdp_src as src3
    on src2.country_name = src3.country_name
    and src2.type = src3.type
    and src2.year = src3.year
),

final as (
    select
        countries_src.country_name,
        type,
        year,
        gdp_nominal,
        gdp_nominal_per_capita,
        gdp_ppp,
        gdp_ppp_per_capita,
        gdp_real,
        population_change
    from stage
    cross join countries_src
    where stage.country_name = countries_src.country_name
    or stage.country_name = countries_src.iso_alpha_2_code
    or stage.country_name = countries_src.iso_alpha_3_code
)

select * from final
