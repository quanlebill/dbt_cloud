with

source    as (select * from {{ ref('stg_fred__fed_funds_target_upper') }}),
countries as (select * from {{ ref('pp_snowflake__countries') }}),

final as (
    select
        countries.country_name,
        source.observation_date,
        source.target_upper
    from source
    join countries on (
        source.country_name = countries.country_name
        or source.country_name = countries.iso_alpha_2_code
        or source.country_name = countries.iso_alpha_3_code
    )
)

select * from final
