{{ config(materialized='table') }}
with
month_spine as (
    {{
        dbt_utils.date_spine(
            datepart = 'month',
            start_date = "date_trunc('month', cast('1947-01-01' as date))",
            end_date   = "date_trunc('month', current_date())"
        )
    }}
),

final as (
    select
        date_month as order_month
    from month_spine
)

select * from final
