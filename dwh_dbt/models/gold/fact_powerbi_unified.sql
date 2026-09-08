{{ config(
    materialized='table',
    schema='gold',
    post_hook=[
        "create index if not exists ix_fact_powerbi_unified_metric on {{ this }} (metric_name)",
        "create index if not exists ix_fact_powerbi_unified_date on {{ this }} (metric_date)",
        "create index if not exists ix_fact_powerbi_unified_resource on {{ this }} (resource_id)",
        "create index if not exists ix_fact_powerbi_unified_repository on {{ this }} (repository_id)"
    ]
) }}

-- One physical fact table for the Power BI semantic model.
-- The grain is explicitly carried by grain_type; metric families are stacked
-- with UNION ALL, never joined to facts at incompatible grains.
-- High-cardinality URLs, comments and source hashes are deliberately omitted.

with stock_population as (

    select distinct on (trim(resource_id))
        trim(resource_id) as resource_id
        , isrc
        , repository_id
        , project_id
        , sub_project_id
        , distribution_team
        , stock_status
        , used_video_count
        , 'dim_stock'::text as population_source
    from {{ ref('mart_resource_usage') }}
    order by trim(resource_id), mart_resource_usage_sk

),

editing_ids as (

    -- fact_editing contains a small number of malformed HG codes. Strip only
    -- separators/punctuation and preserve the canonical HGFA + hex payload.
    select distinct
        regexp_replace(upper(trim(hg_stock_id)), '[^0-9A-Z]', '', 'g') as resource_id
    from {{ ref('fact_editing') }}
    where nullif(trim(hg_stock_id), '') is not null

),

editing_only_population as (

    select
        e.resource_id
        , null::text as isrc
        , null::text as repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , null::text as distribution_team
        , 'Chỉ có Editing'::text as stock_status
        , greatest(count(distinct u.video_id), 1)::bigint as used_video_count
        , 'fact_editing_only'::text as population_source
    from editing_ids e
    left join stock_population s
        on e.resource_id = upper(s.resource_id)
    left join {{ ref('resources_useage_number') }} u
        on e.resource_id = regexp_replace(
            upper(trim(u.hg_stock_id)), '[^0-9A-Z]', '', 'g'
        )
    where s.resource_id is null
      and e.resource_id ~ '^HGFA[0-9A-F]+$'
    group by e.resource_id

),

resource_population as (
    select * from stock_population
    union all
    select * from editing_only_population
),

resource_snapshot as (

    select
        current_date as metric_date
        , 'resource_snapshot'::text as grain_type
        , m.metric_name::text
        , m.metric_value::numeric(38, 4)
        , r.resource_id
        , null::text as video_id
        , r.isrc
        , null::text as channel_id
        , null::text as platform
        , r.repository_id
        , r.project_id
        , r.sub_project_id
        , r.distribution_team as team
        , null::text as process_step
        , r.stock_status as status
        , null::text as position_group
    from resource_population r
    cross join lateral (
        values
            ('resource_count', 1::numeric)
            , ('mapped_resource_count', case when r.repository_id is not null then 1::numeric else 0::numeric end)
            , ('used_resource_count_all_links', case when r.used_video_count > 0 then 1::numeric else 0::numeric end)
            , ('unused_resource_count_all_links', case when r.used_video_count > 0 then 0::numeric else 1::numeric end)
            , ('linked_video_count_all', r.used_video_count::numeric)
            , ('cold_resource_count', case when r.stock_status = 'Hàng nguội' then 1::numeric else 0::numeric end)
            , ('inventory_resource_count', case when r.stock_status = 'Tồn kho' then 1::numeric else 0::numeric end)
            , ('stored_resource_count', case when r.stock_status = 'Lưu kho' then 1::numeric else 0::numeric end)
    ) m(metric_name, metric_value)

),

resource_usage_month as (

    select
        date_trunc('month', published_date)::date as metric_date
        , 'resource_month_position'::text as grain_type
        , 'published_resource_video_uses'::text as metric_name
        , count(*)::numeric(38, 4) as metric_value
        , regexp_replace(upper(trim(hg_stock_id)), '[^0-9A-Z]', '', 'g') as resource_id
        , null::text as video_id
        , null::text as isrc
        , null::text as channel_id
        , null::text as platform
        , null::text as repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , null::text as team
        , null::text as process_step
        , null::text as status
        , position_group
    from {{ ref('resources_useage_number') }}
    group by
        date_trunc('month', published_date)::date
        , regexp_replace(upper(trim(hg_stock_id)), '[^0-9A-Z]', '', 'g')
        , position_group

),

resource_performance_month as (

    select
        date_trunc('month', r.record_date)::date as metric_date
        , 'resource_month'::text as grain_type
        , m.metric_name::text
        , m.metric_value::numeric(38, 4)
        , trim(r.resource_id) as resource_id
        , null::text as video_id
        , null::text as isrc
        , null::text as channel_id
        , null::text as platform
        , null::text as repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , null::text as team
        , null::text as process_step
        , null::text as status
        , null::text as position_group
    from (
        select
            resource_id
            , date_trunc('month', record_date)::date as record_date
            , sum("view") as views
            , sum(revenue) as revenue
        from {{ ref('mart_resource_channel') }}
        where record_date is not null
        group by resource_id, date_trunc('month', record_date)::date
    ) r
    cross join lateral (
        values
            ('allocated_resource_views', r.views::numeric)
            , ('allocated_resource_revenue', r.revenue::numeric)
    ) m(metric_name, metric_value)

),

video_performance_month as (

    select
        v.revenue_month as metric_date
        , 'repository_video_month'::text as grain_type
        , m.metric_name::text
        , m.metric_value::numeric(38, 4)
        , null::text as resource_id
        , v.video_id
        , null::text as isrc
        , null::text as channel_id
        , null::text as platform
        , v.repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , null::text as team
        , null::text as process_step
        , null::text as status
        , null::text as position_group
    from {{ ref('mart_video_performance_monthly') }} v
    cross join lateral (
        values
            ('video_views', v.view_count::numeric)
            , ('video_revenue', v.revenue_amount::numeric)
    ) m(metric_name, metric_value)

),

distro_performance_month as (

    select
        to_date(d.revenue_month || '-01', 'YYYY-MM-DD') as metric_date
        , 'isrc_platform_month'::text as grain_type
        , m.metric_name::text
        , m.metric_value::numeric(38, 4)
        , null::text as resource_id
        , null::text as video_id
        , d.isrc
        , null::text as channel_id
        , d.platform
        , d.repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , null::text as team
        , null::text as process_step
        , null::text as status
        , null::text as position_group
    from {{ ref('fact_revenue_stream_distro') }} d
    cross join lateral (
        values
            ('distro_streams', d.stream_count::numeric)
            , ('distro_revenue', d.revenue_amount::numeric)
    ) m(metric_name, metric_value)

),

resource_cost_month as (

    select
        date_trunc('month', c.incurred_datetime)::date as metric_date
        , 'resource_month'::text as grain_type
        , 'purchase_cost'::text as metric_name
        , sum(c.additional_cost)::numeric(38, 4) as metric_value
        , trim(c.resource_id) as resource_id
        , null::text as video_id
        , null::text as isrc
        , null::text as channel_id
        , null::text as platform
        , null::text as repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , null::text as team
        , null::text as process_step
        , null::text as status
        , null::text as position_group
    from {{ ref('fact_purchase_cost') }} c
    group by date_trunc('month', c.incurred_datetime)::date, trim(c.resource_id)

),

repository_snapshot as (

    select
        current_date as metric_date
        , 'repository_snapshot'::text as grain_type
        , m.metric_name::text
        , m.metric_value::numeric(38, 4)
        , null::text as resource_id
        , null::text as video_id
        , null::text as isrc
        , null::text as channel_id
        , null::text as platform
        , r.repository_id
        , r.project_id
        , r.sub_project_id
        , null::text as team
        , null::text as process_step
        , null::text as status
        , null::text as position_group
    from {{ ref('mart_repository_usage') }} r
    cross join lateral (
        values
            ('repository_resources', r.number_resources::numeric)
            , ('repository_used_resources', r.used_resources::numeric)
            , ('repository_usage_count', r.total_resource_usage_published::numeric)
            , ('repository_revenue', r.revenue::numeric)
            , ('repository_standard_cost', r.standard_cost::numeric)
            , ('repository_purchase_cost', r.purchase_cost::numeric)
            , ('repository_total_cost', r.total_cost::numeric)
    ) m(metric_name, metric_value)

),

team_snapshot as (

    select
        current_date as metric_date
        , 'team_snapshot'::text as grain_type
        , m.metric_name::text
        , m.metric_value::numeric(38, 4)
        , null::text as resource_id
        , null::text as video_id
        , null::text as isrc
        , null::text as channel_id
        , null::text as platform
        , null::text as repository_id
        , null::text as project_id
        , null::text as sub_project_id
        , t.team
        , null::text as process_step
        , null::text as status
        , null::text as position_group
    from {{ ref('mart_team_usage') }} t
    cross join lateral (
        values
            ('team_distributed_resources', t.distributed_resources::numeric)
            , ('team_used_resources', t.usage_resources::numeric)
            , ('team_views', t."view"::numeric)
            , ('team_revenue', t.revenue::numeric)
    ) m(metric_name, metric_value)

),

process_funnel as (

    select
        f.cohort_date::date as metric_date
        , 'process_item'::text as grain_type
        , 'process_quantity'::text as metric_name
        , f.so_luong::numeric(38, 4) as metric_value
        , coalesce(f.hg_stock_id, f.resource_id) as resource_id
        , null::text as video_id
        , f.isrc
        , f.channel_id
        , f.platform
        , f.repository_id
        , coalesce(f.stock_project_id, f.production_project_id, f.project_id) as project_id
        , coalesce(f.stock_sub_project_id, f.production_sub_project_id, f.sub_project_id)
            as sub_project_id
        , null::text as team
        , f.buoc as process_step
        , f.tinh_trang as status
        , null::text as position_group
    from {{ ref('fact_hao_hut') }} f

),

resource_context as (

    -- Denormalize the resource hierarchy onto every resource-grain metric so
    -- Power BI slicers work without importing a separate dimension table.
    select distinct on (trim(resource_id))
        trim(resource_id) as resource_id
        , isrc
        , repository_id
        , project_id
        , sub_project_id
        , distribution_team as team
        , stock_status as status
    from resource_population
    where nullif(trim(resource_id), '') is not null
    order by trim(resource_id), population_source

),

unified as (
    select * from resource_snapshot
    union all select * from resource_usage_month
    union all select * from resource_performance_month
    union all select * from video_performance_month
    union all select * from distro_performance_month
    union all select * from resource_cost_month
    union all select * from repository_snapshot
    union all select * from team_snapshot
    union all select * from process_funnel
)

select
    row_number() over (
        order by u.grain_type, u.metric_name, u.metric_date, u.resource_id,
                 u.video_id, coalesce(u.repository_id, rc.repository_id),
                 coalesce(u.isrc, rc.isrc), u.platform,
                 coalesce(u.team, rc.team), u.process_step,
                 coalesce(u.status, rc.status), u.position_group
    )::bigint as fact_key
    , u.metric_date
    , u.grain_type
    , u.metric_name
    , u.metric_value
    , u.resource_id
    , u.video_id
    , coalesce(u.isrc, rc.isrc) as isrc
    , u.channel_id
    , u.platform
    , coalesce(u.repository_id, rc.repository_id) as repository_id
    , coalesce(u.project_id, rc.project_id) as project_id
    , coalesce(u.sub_project_id, rc.sub_project_id) as sub_project_id
    , coalesce(u.team, rc.team) as team
    , u.process_step
    , coalesce(u.status, rc.status) as status
    , u.position_group
from unified u
left join resource_context rc
    on u.resource_id = rc.resource_id
where u.metric_value is not null
  and u.metric_value <> 0
