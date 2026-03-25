-- Consumer Sentiment monthly — standalone; different aggregate time from daily series
with

source as (select * from {{ ref('stg_fred__consumer_sentiment') }})

select * from source
