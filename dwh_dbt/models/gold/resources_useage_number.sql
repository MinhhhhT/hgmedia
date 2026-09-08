{{ config(
    materialized='table',
    schema='gold'
) }}

with published_videos as (

    select distinct
        video_id
        , editing_code
        , published_date
    from {{ ref('dim_video') }}
    where published_date is not null
        and video_id is not null
        and editing_code is not null

),

resource_video_pairs as (

    select
        pv.video_id
        , pv.published_date
        , fe.hg_stock_id
        , min(fe.position) as first_position
    from published_videos pv
    inner join {{ ref('fact_editing') }} fe
        on fe.editing_code = pv.editing_code
    where fe.hg_stock_id is not null
    group by
        pv.video_id
        , pv.published_date
        , fe.hg_stock_id

)

select
    {{ dbt_utils.generate_surrogate_key([
        'video_id',
        'hg_stock_id'
    ]) }} as resource_video_sk
    , video_id
    , hg_stock_id
    , published_date
    , first_position
    , case
        when first_position = 1 then '1'
        when first_position between 2 and 3 then '2-3'
        when first_position between 4 and 5 then '4-5'
        when first_position > 5 then '>5'
      end as position_group
from resource_video_pairs