{{ config(
    materialized='table',
    schema='gold',
    post_hook=[
        "create unique index if not exists ux_mart_powerbi_resource_obt_resource_id on {{ this }} (resource_id)",
        "create index if not exists ix_mart_powerbi_resource_obt_repository_id on {{ this }} (repository_id)",
        "create index if not exists ix_mart_powerbi_resource_obt_project_id on {{ this }} (project_id)",
        "create index if not exists ix_mart_powerbi_resource_obt_stock_status on {{ this }} (stock_status)"
    ]
) }}

-- Power BI import mart.
-- Grain: exactly one row per normalized HG stock resource ID.
--
-- Only additive/snapshot metrics compatible with resource grain belong here.
-- Daily/monthly growth, distinct videos across multiple resources, team totals,
-- SLA, and hao-hut process steps have different grains and must not be copied
-- onto every resource row.

with resource_base as (

    select
        trim(resource_id) as resource_id
        , resource_name
        , stock_name
        , isrc
        , resource_origin
        , source
        , repository
        , repository_id
        , sub_project_name
        , sub_project_id
        , project
        , project_id
        , stock_status
        , stock_stored_date
        , resource_age_days
        , distribution_team
        , last_distribution_date
        , ar_name
        , seo_name
        , used_video_count as linked_video_count_all
        , last_used_published_date
        , days_since_last_usage
        , inventory_unused_days
    from {{ ref('mart_resource_usage') }}

),

resource_financial_context as (

    -- A resource can occur in dim_resources more than once. Pick one current
    -- business mapping before joining so metrics can never fan out.
    select distinct on (trim(hg_stock_id))
        trim(hg_stock_id) as resource_id
        , acceptance_score
        , acceptance_cost as standard_cost
        , acceptance_date
    from {{ ref('dim_resources') }}
    where nullif(trim(hg_stock_id), '') is not null
    order by
        trim(hg_stock_id)
        , case when repository_id is not null then 0 else 1 end
        , case when odoo_id is not null then 0 else 1 end
        , acceptance_date desc nulls last
        , dim_resources_sk

),

resource_performance as (

    -- mart_resource_channel is daily. Collapse it before the OBT join.
    select
        trim(resource_id) as resource_id
        , sum(coalesce("view", 0))::numeric(38, 4) as lifetime_views
        , sum(coalesce(revenue, 0))::numeric(38, 4) as lifetime_revenue
        , min(record_date) as first_metric_date
        , max(record_date) as last_metric_date
        , max(channel_use)::bigint as published_channel_count
        , max(video_use)::bigint as published_video_count_from_performance
    from {{ ref('mart_resource_channel') }}
    group by trim(resource_id)

),

video_dimension as (

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

published_usage as (

    -- resources_useage_number is already unique at resource x published video.
    select
        trim(u.hg_stock_id) as resource_id
        , count(*)::bigint as published_resource_video_uses
        , count(*) filter (where u.first_position = 1)::bigint as uses_position_1
        , count(*) filter (where u.first_position between 2 and 3)::bigint as uses_position_2_3
        , count(*) filter (where u.first_position between 4 and 5)::bigint as uses_position_4_5
        , count(*) filter (where u.first_position > 5)::bigint as uses_position_gt_5
        , count(distinct v.channel_id)::bigint as published_channel_count
        , min(u.published_date)::date as first_used_published_date
        , max(u.published_date)::date as last_used_published_date
    from {{ ref('resources_useage_number') }} u
    left join video_dimension v
        on trim(u.video_id) = v.video_id
    group by trim(u.hg_stock_id)

),

purchase_cost as (

    select
        trim(resource_id) as resource_id
        , sum(coalesce(additional_cost, 0))::numeric(38, 4) as purchase_cost
    from {{ ref('fact_purchase_cost') }}
    where nullif(trim(resource_id), '') is not null
    group by trim(resource_id)

),

metric_rows as (

    -- Stack pre-aggregated metric families, then aggregate once more. This
    -- avoids a chain of joins whose cardinality PostgreSQL can overestimate
    -- by several orders of magnitude while creating the table.
    select
        resource_id
        , lifetime_views
        , lifetime_revenue
        , first_metric_date
        , last_metric_date
        , published_channel_count as performance_channel_count
        , cast(null as bigint) as published_resource_video_uses
        , cast(null as bigint) as uses_position_1
        , cast(null as bigint) as uses_position_2_3
        , cast(null as bigint) as uses_position_4_5
        , cast(null as bigint) as uses_position_gt_5
        , cast(null as bigint) as usage_channel_count
        , cast(null as date) as first_used_published_date
        , cast(null as date) as last_used_published_date
        , cast(null as numeric(38, 4)) as standard_cost
        , cast(null as numeric(38, 4)) as purchase_cost
        , cast(null as numeric) as acceptance_score
        , cast(null as date) as acceptance_date
    from resource_performance

    union all

    select
        resource_id
        , null::numeric(38, 4)
        , null::numeric(38, 4)
        , null::date
        , null::date
        , null::bigint
        , published_resource_video_uses
        , uses_position_1
        , uses_position_2_3
        , uses_position_4_5
        , uses_position_gt_5
        , published_channel_count
        , first_used_published_date
        , last_used_published_date
        , null::numeric(38, 4)
        , null::numeric(38, 4)
        , null::numeric
        , null::date
    from published_usage

    union all

    select
        resource_id
        , null::numeric(38, 4)
        , null::numeric(38, 4)
        , null::date
        , null::date
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::date
        , null::date
        , standard_cost::numeric(38, 4)
        , null::numeric(38, 4)
        , acceptance_score
        , acceptance_date::date
    from resource_financial_context

    union all

    select
        resource_id
        , null::numeric(38, 4)
        , null::numeric(38, 4)
        , null::date
        , null::date
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::bigint
        , null::date
        , null::date
        , null::numeric(38, 4)
        , purchase_cost
        , null::numeric
        , null::date
    from purchase_cost

),

resource_metrics as (

    select
        resource_id
        , sum(lifetime_views) as lifetime_views
        , sum(lifetime_revenue) as lifetime_revenue
        , min(first_metric_date) as first_metric_date
        , max(last_metric_date) as last_metric_date
        , max(performance_channel_count) as performance_channel_count
        , sum(published_resource_video_uses) as published_resource_video_uses
        , sum(uses_position_1) as uses_position_1
        , sum(uses_position_2_3) as uses_position_2_3
        , sum(uses_position_4_5) as uses_position_4_5
        , sum(uses_position_gt_5) as uses_position_gt_5
        , max(usage_channel_count) as usage_channel_count
        , min(first_used_published_date) as first_used_published_date
        , max(last_used_published_date) as last_used_published_date
        , max(standard_cost) as standard_cost
        , sum(purchase_cost) as purchase_cost
        , max(acceptance_score) as acceptance_score
        , max(acceptance_date) as acceptance_date
    from metric_rows
    group by resource_id

),

final as (

    select
        row_number() over (order by rb.resource_id)::bigint as resource_key
        , rb.resource_id

        -- Descriptive columns. URLs and high-cardinality technical hashes are
        -- deliberately excluded to improve VertiPaq compression.
        , coalesce(rb.resource_name, rb.stock_name) as resource_name
        , rb.isrc
        , rb.resource_origin
        , rb.source as repository_type
        , rb.repository_id
        , rb.repository
        , rb.sub_project_id
        , rb.sub_project_name
        , rb.project_id
        , rb.project
        , rb.stock_status
        , rb.distribution_team
        , rb.ar_name
        , rb.seo_name

        -- Dates and lifecycle snapshots.
        , rb.stock_stored_date
        , rm.acceptance_date
        , rb.last_distribution_date
        , rm.first_used_published_date
        , coalesce(rm.last_used_published_date, rb.last_used_published_date)
            as last_used_published_date
        , rm.first_metric_date
        , rm.last_metric_date
        , rb.resource_age_days
        , rb.days_since_last_usage
        , rb.inventory_unused_days

        -- Additive resource-grain measures.
        , 1::smallint as resource_count
        , case when rb.repository_id is not null then 1 else 0 end::smallint
            as mapped_resource_count
        , case when coalesce(rm.published_resource_video_uses, 0) > 0 then 1 else 0 end::smallint
            as used_resource_count
        , case when coalesce(rm.published_resource_video_uses, 0) = 0 then 1 else 0 end::smallint
            as unused_resource_count
        , coalesce(rb.linked_video_count_all, 0)::bigint as linked_video_count_all
        , coalesce(rm.published_resource_video_uses, 0)::bigint
            as published_resource_video_uses
        , coalesce(rm.usage_channel_count, rm.performance_channel_count, 0)::bigint
            as published_channel_count
        , coalesce(rm.uses_position_1, 0)::bigint as uses_position_1
        , coalesce(rm.uses_position_2_3, 0)::bigint as uses_position_2_3
        , coalesce(rm.uses_position_4_5, 0)::bigint as uses_position_4_5
        , coalesce(rm.uses_position_gt_5, 0)::bigint as uses_position_gt_5
        , coalesce(rm.lifetime_views, 0)::numeric(38, 4) as lifetime_views
        , coalesce(rm.lifetime_revenue, 0)::numeric(38, 4) as lifetime_revenue
        , coalesce(rm.standard_cost, 0)::numeric(38, 4) as standard_cost
        , coalesce(rm.purchase_cost, 0)::numeric(38, 4) as purchase_cost
        , (
            coalesce(rm.standard_cost, 0)
            + coalesce(rm.purchase_cost, 0)
          )::numeric(38, 4) as total_cost
        , rm.acceptance_score

    from resource_base rb
    left join resource_metrics rm
        on rb.resource_id = rm.resource_id

)

select * from final
