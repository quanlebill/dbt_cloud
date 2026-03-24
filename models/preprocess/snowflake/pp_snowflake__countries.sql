with
source as (
    select * from {{ref("stg_snowflake__countries")}}
),

final as (
    select * from source
)
select * from final

