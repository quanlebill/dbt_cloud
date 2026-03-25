-- Monthly fed funds effective rate — kept separate from daily due to different aggregate time
with

source as (select * from {{ ref('stg_fred__fed_funds_monthly') }})

select * from source
