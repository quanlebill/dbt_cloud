-- Yield curve rates and recession spreads are both daily; joined into one wide table
with

rates     as (select * from {{ ref('stg_fred__treasury_yield_curve') }}),
spreads   as (select * from {{ ref('stg_fred__yield_curve_spreads') }}),
countries as (select * from {{ ref('pp_snowflake__countries') }}),

stage as (
    select
        coalesce(r.country_name, s.country_name)         as country_name,
        coalesce(r.observation_date, s.observation_date) as observation_date,
        r.y1m, r.y3m, r.y6m, r.y1, r.y2, r.y5, r.y10, r.y30,
        s.spread_10y_2y,
        s.spread_10y_3m
    from rates r
    full join spreads s on r.country_name     = s.country_name
                       and r.observation_date = s.observation_date
    where coalesce(r.observation_date, s.observation_date) is not null
),

final as (
    select
        countries.country_name,
        stage.observation_date,
        stage.y1m, stage.y3m, stage.y6m, stage.y1, stage.y2,
        stage.y5, stage.y10, stage.y30,
        stage.spread_10y_2y,
        stage.spread_10y_3m
    from stage
    join countries on (
        stage.country_name = countries.country_name
        or stage.country_name = countries.iso_alpha_2_code
        or stage.country_name = countries.iso_alpha_3_code
    )
)

select * from final
