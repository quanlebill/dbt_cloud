with 

source as (

    select * from {{ source('snowflake', 'countries') }}

),

final as (

    select
        name,
        iso_alpha_2_code,
        iso_alpha_3_code

    from source

)

select * from final