with

source    as (select * from {{ ref('stg_fred__dgs1mo') }}),
countries as (select * from {{ ref('pp_snowflake__countries') }}),

final as (
    select
        countries.country_name,
        source.observation_date,
        source.yield_1mo
    from source
    join countries on (
        source.country_name = countries.country_name
        or source.country_name = countries.iso_alpha_2_code
        or source.country_name = countries.iso_alpha_3_code
    )
)

select * from final
