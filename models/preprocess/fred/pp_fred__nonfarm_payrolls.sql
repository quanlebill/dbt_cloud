-- Non-Farm Payrolls monthly — standalone; different aggregate time from daily series
with

source as (select * from {{ ref('stg_fred__nonfarm_payrolls') }})

select * from source
