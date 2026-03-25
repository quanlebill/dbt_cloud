-- Yield curve rates and recession spreads are both daily; joined into one wide table
with

rates   as (select * from {{ ref('stg_fred__treasury_yield_curve') }}),
spreads as (select * from {{ ref('stg_fred__yield_curve_spreads') }}),

stage as (
    select
        coalesce(r.observation_date, s.observation_date) as observation_date,
        r.y1m,
        r.y3m,
        r.y6m,
        r.y1,
        r.y2,
        r.y5,
        r.y10,
        r.y30,
        s.spread_10y_2y,
        s.spread_10y_3m
    from rates r
    full join spreads s on r.observation_date = s.observation_date
    where coalesce(r.observation_date, s.observation_date) is not null
)

select * from stage
