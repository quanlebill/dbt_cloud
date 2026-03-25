-- Joins 8 daily Treasury yield series into a single yield curve table
with

dgs1mo_src  as (select * from {{ source('fred', 'dgs1mo') }}),
dgs3mo_src  as (select * from {{ source('fred', 'dgs3mo') }}),
dgs6mo_src  as (select * from {{ source('fred', 'dgs6mo') }}),
dgs1_src    as (select * from {{ source('fred', 'dgs1')   }}),
dgs2_src    as (select * from {{ source('fred', 'dgs2')   }}),
dgs5_src    as (select * from {{ source('fred', 'dgs5')   }}),
dgs10_src   as (select * from {{ source('fred', 'dgs10')  }}),
dgs30_src   as (select * from {{ source('fred', 'dgs30')  }}),

y1m  as (select cast(date as date) as d, cast(value as float) as y1m  from dgs1mo_src  where value is not null and value != '.'),
y3m  as (select cast(date as date) as d, cast(value as float) as y3m  from dgs3mo_src  where value is not null and value != '.'),
y6m  as (select cast(date as date) as d, cast(value as float) as y6m  from dgs6mo_src  where value is not null and value != '.'),
y1   as (select cast(date as date) as d, cast(value as float) as y1   from dgs1_src    where value is not null and value != '.'),
y2   as (select cast(date as date) as d, cast(value as float) as y2   from dgs2_src    where value is not null and value != '.'),
y5   as (select cast(date as date) as d, cast(value as float) as y5   from dgs5_src    where value is not null and value != '.'),
y10  as (select cast(date as date) as d, cast(value as float) as y10  from dgs10_src   where value is not null and value != '.'),
y30  as (select cast(date as date) as d, cast(value as float) as y30  from dgs30_src   where value is not null and value != '.'),

final as (
    select
        coalesce(y1m.d, y3m.d, y6m.d, y1.d, y2.d, y5.d, y10.d, y30.d) as observation_date,
        y1m.y1m,
        y3m.y3m,
        y6m.y6m,
        y1.y1,
        y2.y2,
        y5.y5,
        y10.y10,
        y30.y30
    from y10
    full join y1m  on y10.d = y1m.d
    full join y3m  on y10.d = y3m.d
    full join y6m  on y10.d = y6m.d
    full join y1   on y10.d = y1.d
    full join y2   on y10.d = y2.d
    full join y5   on y10.d = y5.d
    full join y30  on y10.d = y30.d
    where coalesce(y1m.d, y3m.d, y6m.d, y1.d, y2.d, y5.d, y10.d, y30.d) is not null
)

select * from final
