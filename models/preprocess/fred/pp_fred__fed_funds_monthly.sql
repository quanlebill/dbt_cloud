-- Monthly fed funds effective rate — kept separate from daily due to different aggregate time
with

source    as (select * from {{ ref('stg_fred__fed_funds_monthly') }}),
countries as (select * from {{ ref('pp_snowflake__countries') }}),

final as (
    select
        countries.country_name,
        source.observation_date,
        source.fed_funds_rate
    from source
    join countries on (
        source.country_name = countries.country_name
        or source.country_name = countries.iso_alpha_2_code
        or source.country_name = countries.iso_alpha_3_code
    )
)

select * from final
