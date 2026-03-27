-- All 7 monthly inflation series joined on country_name + observation_date into one wide table
with

cpi          as (select * from {{ ref('pp_fred__cpi') }}),
core_cpi     as (select * from {{ ref('pp_fred__core_cpi') }}),
pce          as (select * from {{ ref('pp_fred__pce') }}),
core_pce     as (select * from {{ ref('pp_fred__core_pce') }}),
trimmed_pce  as (select * from {{ ref('pp_fred__trimmed_mean_pce') }}),
median_cpi   as (select * from {{ ref('pp_fred__median_cpi') }}),
ppi          as (select * from {{ ref('pp_fred__ppi') }}),

final as (
    select
        coalesce(
            cpi.country_name, core_cpi.country_name, pce.country_name,
            core_pce.country_name, trimmed_pce.country_name,
            median_cpi.country_name, ppi.country_name
        )                               as country_name,
        coalesce(
            cpi.observation_date, core_cpi.observation_date, pce.observation_date,
            core_pce.observation_date, trimmed_pce.observation_date,
            median_cpi.observation_date, ppi.observation_date
        )                               as observation_date,
        cpi.cpi_level,
        core_cpi.core_cpi_level,
        pce.pce_level,
        core_pce.core_pce_level,
        trimmed_pce.trimmed_mean_pce_yoy,
        median_cpi.median_cpi_yoy,
        ppi.ppi_level
    from cpi
    full join core_cpi    on cpi.country_name = core_cpi.country_name
                         and cpi.observation_date = core_cpi.observation_date
    full join pce         on cpi.country_name = pce.country_name
                         and cpi.observation_date = pce.observation_date
    full join core_pce    on cpi.country_name = core_pce.country_name
                         and cpi.observation_date = core_pce.observation_date
    full join trimmed_pce on cpi.country_name = trimmed_pce.country_name
                         and cpi.observation_date = trimmed_pce.observation_date
    full join median_cpi  on cpi.country_name = median_cpi.country_name
                         and cpi.observation_date = median_cpi.observation_date
    full join ppi         on cpi.country_name = ppi.country_name
                         and cpi.observation_date = ppi.observation_date
)

select * from final
