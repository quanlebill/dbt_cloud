-- All 7 inflation series are monthly; joined on observation_date into one wide table
with

cpi          as (select * from {{ ref('stg_fred__cpi') }}),
core_cpi     as (select * from {{ ref('stg_fred__core_cpi') }}),
pce          as (select * from {{ ref('stg_fred__pce') }}),
core_pce     as (select * from {{ ref('stg_fred__core_pce') }}),
trimmed_pce  as (select * from {{ ref('stg_fred__trimmed_mean_pce') }}),
median_cpi   as (select * from {{ ref('stg_fred__median_cpi') }}),
ppi          as (select * from {{ ref('stg_fred__ppi') }}),

stage as (
    select
        coalesce(
            cpi.observation_date,
            core_cpi.observation_date,
            pce.observation_date,
            core_pce.observation_date,
            trimmed_pce.observation_date,
            median_cpi.observation_date,
            ppi.observation_date
        )                               as observation_date,
        cpi.cpi_level,
        core_cpi.core_cpi_level,
        pce.pce_level,
        core_pce.core_pce_level,
        trimmed_pce.trimmed_mean_pce_yoy,
        median_cpi.median_cpi_yoy,
        ppi.ppi_level
    from cpi
    full join core_cpi    on cpi.observation_date = core_cpi.observation_date
    full join pce         on cpi.observation_date = pce.observation_date
    full join core_pce    on cpi.observation_date = core_pce.observation_date
    full join trimmed_pce on cpi.observation_date = trimmed_pce.observation_date
    full join median_cpi  on cpi.observation_date = median_cpi.observation_date
    full join ppi         on cpi.observation_date = ppi.observation_date
)

select * from stage
