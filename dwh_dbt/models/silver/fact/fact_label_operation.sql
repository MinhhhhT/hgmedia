with base as (
    select *
    from {{ source('staging', 'x_music_song') }}
    where active = true
      and nullif(trim(cast(isrc as text)), '') is not null
)

select distinct on (b.isrc)
    {{ dbt_utils.generate_surrogate_key(['b.id']) }} as fact_label_operation_sk,
    nullif(trim(cast(b.id as text)), '') as resource_id,
    nullif(trim(cast(b.release_state as text)), '') as release_status,
    cast(nullif(trim(cast(b.release_date as text)), '') as timestamp) as release_date,
    nullif(trim(cast(ac.acceptance_link as text)), '') as acceptance_url,
    nullif(trim(cast(b.isrc as text)), '') as isrc,
    nullif(trim(cast(b.distro as text)), '') as distributor
from base b
left join {{ source('staging', 'x_acceptance_cert') }} ac
    on cast(b.purchase_order_id as bigint) = cast(ac.purchase_order_id as bigint)
order by
    b.isrc,
    b.id asc
