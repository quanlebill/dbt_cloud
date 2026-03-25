{{ config(materialized='table') }}
with
day_spine as (
    {{
        dbt_utils.date_spine(
            datepart = 'day',
            start_date = "cast('1962-01-01' as date)",
            end_date   = "current_date()"
        )
    }}
),

final as (
    select
        date_day as order_day
    from day_spine
)

select * from final
