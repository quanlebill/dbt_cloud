-- Daily fed funds effective rate joined with FOMC target corridor (lower + upper)
with

daily_rate   as (select * from {{ ref('pp_fred__fed_funds_daily') }}),
target_lower as (select * from {{ ref('pp_fred__fed_funds_target_lower') }}),
target_upper as (select * from {{ ref('pp_fred__fed_funds_target_upper') }}),

final as (
    select
        coalesce(daily_rate.country_name, target_lower.country_name, target_upper.country_name) as country_name,
        coalesce(daily_rate.observation_date, target_lower.observation_date, target_upper.observation_date) as observation_date,
        daily_rate.fed_funds_rate,
        target_lower.target_lower,
        target_upper.target_upper
    from daily_rate
    full join target_lower on daily_rate.country_name     = target_lower.country_name
                          and daily_rate.observation_date = target_lower.observation_date
    full join target_upper on daily_rate.country_name     = target_upper.country_name
                          and daily_rate.observation_date = target_upper.observation_date
    where coalesce(daily_rate.observation_date, target_lower.observation_date, target_upper.observation_date) is not null
)

select * from final
