{{ config(materialized='table') }}
with

source as (
    select * from {{ ref('pp_fred__inflation') }}
),

final as (
    select
        cast(observation_date as varchar)                             as inflation_pii,
        country_name,
        cast(date_trunc('month', observation_date) as timestamp_ntz) as observation_date,
        cpi_level,
        core_cpi_level,
        pce_level,
        core_pce_level,
        trimmed_mean_pce_yoy,
        median_cpi_yoy,
        ppi_level
    from source
)

select * from final
order by observation_date desc
