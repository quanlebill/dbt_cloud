-- Daily Treasury yield curve (8 maturities) joined with 10Y-2Y and 10Y-3M recession spreads
with

dgs1mo  as (select * from {{ ref('pp_fred__dgs1mo') }}),
dgs3mo  as (select * from {{ ref('pp_fred__dgs3mo') }}),
dgs6mo  as (select * from {{ ref('pp_fred__dgs6mo') }}),
dgs1y   as (select * from {{ ref('pp_fred__dgs1y') }}),
dgs2y   as (select * from {{ ref('pp_fred__dgs2y') }}),
dgs5y   as (select * from {{ ref('pp_fred__dgs5y') }}),
dgs10y  as (select * from {{ ref('pp_fred__dgs10y') }}),
dgs30y  as (select * from {{ ref('pp_fred__dgs30y') }}),
t10y2y  as (select * from {{ ref('pp_fred__t10y2y') }}),
t10y3m  as (select * from {{ ref('pp_fred__t10y3m') }}),

final as (
    select
        coalesce(
            dgs10y.country_name, dgs1mo.country_name, dgs3mo.country_name,
            dgs6mo.country_name, dgs1y.country_name, dgs2y.country_name,
            dgs5y.country_name, dgs30y.country_name, t10y2y.country_name, t10y3m.country_name
        )                               as country_name,
        coalesce(
            dgs1mo.observation_date, dgs3mo.observation_date, dgs6mo.observation_date,
            dgs1y.observation_date, dgs2y.observation_date, dgs5y.observation_date,
            dgs10y.observation_date, dgs30y.observation_date
        )                               as observation_date,
        dgs1mo.yield_1mo,
        dgs3mo.yield_3mo,
        dgs6mo.yield_6mo,
        dgs1y.yield_1y,
        dgs2y.yield_2y,
        dgs5y.yield_5y,
        dgs10y.yield_10y,
        dgs30y.yield_30y,
        t10y2y.spread_10y_2y,
        t10y3m.spread_10y_3m
    from dgs10y
    full join dgs1mo  on dgs10y.observation_date = dgs1mo.observation_date
    full join dgs3mo  on dgs10y.observation_date = dgs3mo.observation_date
    full join dgs6mo  on dgs10y.observation_date = dgs6mo.observation_date
    full join dgs1y   on dgs10y.observation_date = dgs1y.observation_date
    full join dgs2y   on dgs10y.observation_date = dgs2y.observation_date
    full join dgs5y   on dgs10y.observation_date = dgs5y.observation_date
    full join dgs30y  on dgs10y.observation_date = dgs30y.observation_date
    full join t10y2y  on dgs10y.observation_date = t10y2y.observation_date
    full join t10y3m  on dgs10y.observation_date = t10y3m.observation_date
    where coalesce(
        dgs1mo.observation_date, dgs3mo.observation_date, dgs6mo.observation_date,
        dgs1y.observation_date, dgs2y.observation_date, dgs5y.observation_date,
        dgs10y.observation_date, dgs30y.observation_date
    ) is not null
)

select * from final
