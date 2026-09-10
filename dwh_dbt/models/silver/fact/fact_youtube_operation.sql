{{ config(
    materialized='incremental',
    unique_key='fact_youtube_operation_sk',
    incremental_strategy='delete+insert'
) }}

with youtube_audio as (
    -- Bat dau tu tracking_video_publish_infos, filter MediaType = 1 (audio)
    select distinct
        t."ResourceFileInfoId" as resource_info_id,
        t."ResourceFileId" as hg_stock_id,
        t."VideoPublishId" as youtube_video_id,
        t."ChannelId" as channel_id,
        t."CreatedDate" as published_date,
        t."VideoType" as video_type,
        nullif(trim(r."ISRC"), '') as isrc
    from staging.tracking_video_publish_infos t
    inner join staging.resource_file_info r
        on t."ResourceFileId" = r."ResourceFileId"
        and t."ResourceFileInfoId" = r."Id"
    where r."MediaType" = 1
        and t."ResourceFileId" is not null
),

enriched as (
    select
        y.resource_info_id,
        y.hg_stock_id,
        y.youtube_video_id,
        y.channel_id,
        y.published_date,
        y.video_type,
        y.isrc,
        nullif(trim(cast(s.cid_state as text)), '') as cid,
        nullif(trim(cast(s.net_cid as text)), '') as net,
        nullif(trim(cast(s.hg_link as text)), '') as stock_url,
        cast(nullif(trim(cast(s.ytb_sent_date as text)), '') as date) as submitted_date
    from youtube_audio y
    left join staging.x_music_song s
        on upper(y.isrc) = upper(nullif(trim(cast(s.isrc as text)), ''))
)

select distinct on (hg_stock_id)
    {{ dbt_utils.generate_surrogate_key(['hg_stock_id']) }} as fact_youtube_operation_sk,
    hg_stock_id,
    resource_info_id,
    isrc,
    cid,
    net,
    stock_url,
    coalesce(submitted_date, cast(published_date as date)) as submitted_date,
    youtube_video_id,
    channel_id,
    video_type
from enriched
order by
    hg_stock_id,
    submitted_date desc nulls last,
    published_date desc nulls last,
    youtube_video_id desc nulls last
