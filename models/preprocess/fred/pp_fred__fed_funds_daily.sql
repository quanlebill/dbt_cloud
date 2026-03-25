-- Daily fed funds effective rate joined with FOMC target range (both daily frequency)
with

daily_rate as (select * from {{ ref('stg_fred__fed_funds_daily') }}),
target     as (select * from {{ ref('stg_fred__fed_funds_target') }}),
countries  as (select * from {{ ref('pp_snowflake__countries') }}),

stage as (
    select
        coalesce(d.country_name, t.country_name)         as country_name,
        coalesce(d.observation_date, t.observation_date) as observation_date,
        d.fed_funds_rate,
        t.target_lower,
        t.target_upper
    from daily_rate d
    full join target t on d.country_name     = t.country_name
                      and d.observation_date = t.observation_date
    where coalesce(d.observation_date, t.observation_date) is not null
),

final as (
    select
        countries.country_name,
        stage.observation_date,
        stage.fed_funds_rate,
        stage.target_lower,
        stage.target_upper
    from stage
    join countries on (
        stage.country_name = countries.country_name
        or stage.country_name = countries.iso_alpha_2_code
        or stage.country_name = countries.iso_alpha_3_code
    )
)

select * from final
