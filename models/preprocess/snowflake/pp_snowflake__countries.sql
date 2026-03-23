with
source as (
    select * from {{ref("stg_snowflake__countries.sql")}}
),
stage as (
    select
        LOWER(country_name) as country_name,
        LOWER(ISO_ALPHA_2_CODE) as ISO_ALPHA_2_CODE,
        LOWER(ISO_ALPHA_3_CODE) as ISO_ALPHA_2_CODE,
    from source
),

final as (
    select * from stage
)
select * from final

