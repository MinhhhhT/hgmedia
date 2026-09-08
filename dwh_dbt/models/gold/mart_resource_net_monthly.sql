{{ config(
    materialized='table',
    schema='gold',
    post_hook=[
        "create unique index if not exists ux_mart_resource_net_monthly_grain on {{ this }} (resource_id, net_id, revenue_month)",
        "create index if not exists ix_mart_resource_net_monthly_month on {{ this }} (revenue_month)",
        "create index if not exists ix_mart_resource_net_monthly_net on {{ this }} (net_id)",
        "create index if not exists ix_mart_resource_net_monthly_resource on {{ this }} (resource_id)"
    ]
) }}

-- Power BI aggregation for resource revenue/view by NET and month.
-- Grain: exactly one row per resource_id x net_id x revenue_month.
--
-- This mart prevents Power BI from materializing a large virtual list of
-- video_id values and scanning fact_revenue_by_resources once per resource.

with stock_resource_bridge as (

    select distinct on (trim(cast(id as text)))
        trim(cast(id as text)) as source_resource_id
        , upper(trim(cast(hg_code as text))) as resource_id
    from {{ source('staging', 'x_music_song') }}
    where nullif(trim(cast(id as text)), '') is not null
        and nullif(trim(cast(hg_code as text)), '') is not null
    order by
        trim(cast(id as text))
        , upper(trim(cast(hg_code as text)))

),

video_dimension as (

    -- A duplicated video row must not multiply allocated revenue.
    select distinct on (trim(cast(video_id as text)))
        trim(cast(video_id as text)) as video_id
        , nullif(trim(cast(channel_id as text)), '') as channel_id
    from {{ ref('dim_video') }}
    where nullif(trim(cast(video_id as text)), '') is not null
    order by
        trim(cast(video_id as text))
        , published_date desc nulls last
        , dim_video_sk

),

channel_dimension as (

    -- A duplicated channel row must not multiply allocated revenue.
    select distinct on (trim(cast(channel_id as text)))
        trim(cast(channel_id as text)) as channel_id
        , nullif(trim(cast(network_id as text)), '') as net_id
    from {{ ref('dim_channel') }}
    where nullif(trim(cast(channel_id as text)), '') is not null
    order by
        trim(cast(channel_id as text))
        , case when nullif(trim(cast(network_id as text)), '') is not null then 0 else 1 end
        , dim_channel_sk

),

normalized_fact as (

    select
        coalesce(
            srb.resource_id,
            upper(trim(cast(f.resource_id as text)))
        ) as resource_id
        , ch.net_id
        , date_trunc('month', f.recorded_date)::date as revenue_month
        , f.revenue_amount
        , f."view"
    from {{ ref('fact_revenue_by_resources') }} f
    inner join video_dimension v
        on trim(cast(f.video_id as text)) = v.video_id
    left join channel_dimension ch
        on v.channel_id = ch.channel_id
    left join stock_resource_bridge srb
        on trim(cast(f.resource_id as text)) = srb.source_resource_id
    where nullif(trim(cast(f.resource_id as text)), '') is not null
        and f.recorded_date is not null

)

select
    resource_id
    , net_id
    , revenue_month
    , sum(coalesce(revenue_amount, 0))::numeric(38, 4) as revenue_amount
    , sum(coalesce("view", 0))::numeric(38, 4) as view_count
from normalized_fact
group by
    resource_id
    , net_id
    , revenue_month
