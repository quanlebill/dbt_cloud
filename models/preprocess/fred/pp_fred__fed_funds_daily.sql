-- Daily fed funds effective rate joined with FOMC target range (both daily frequency)
with

daily_rate as (select * from {{ ref('stg_fred__fed_funds_daily') }}),
target     as (select * from {{ ref('stg_fred__fed_funds_target') }}),

stage as (
    select
        coalesce(d.observation_date, t.observation_date) as observation_date,
        d.fed_funds_rate,
        t.target_lower,
        t.target_upper
    from daily_rate d
    full join target t on d.observation_date = t.observation_date
    where coalesce(d.observation_date, t.observation_date) is not null
)

select * from stage
