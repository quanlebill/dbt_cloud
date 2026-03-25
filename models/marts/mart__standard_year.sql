{{config(materialized='table')}}
with
year_spine as (
    {{
        dbt_utils.date_spine(
            datepart = 'year',
            start_date = "date_trunc('year', dateadd(year, -5, current_timestamp()))",
            end_date = "date_trunc('year', current_timestamp())"
        )
    }}
),

final as (
    select
        date_year as order_year
    from year_spine
)

select * from final