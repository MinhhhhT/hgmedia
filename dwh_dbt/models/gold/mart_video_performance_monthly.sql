-- Mart: hiệu quả hoạt động video theo tháng và kho sở hữu tài nguyên.
-- Grain: repository_id x video_id x revenue_month.
--
-- Revenue và view trong fact_revenue_by_resources đã được phân bổ theo trọng
-- số tài nguyên, vì vậy phải SUM cả hai metric sau khi map resource -> kho.
-- repository_id NULL biểu thị phần metric của tài nguyên chưa map được kho.
-- Không ROUND revenue trong ETL để tổng các kho vẫn bằng đúng 100% của video.

{{ config(materialized='table') }}

with resource_base as (
    -- Chọn đúng một mapping kho cho mỗi hg_stock_id để không nhân metric.
    select distinct on (upper(trim(cast(r.hg_stock_id as text))))
        upper(trim(cast(r.hg_stock_id as text))) as resource_id
        , r.repository_id
    from {{ ref('dim_resources') }} r
    where nullif(trim(cast(r.hg_stock_id as text)), '') is not null
    order by
        upper(trim(cast(r.hg_stock_id as text)))
        , case when r.repository_id is not null then 0 else 1 end
        , case when r.odoo_id is not null then 0 else 1 end
        , r.acceptance_date desc nulls last
        , r.dim_resources_sk
),

video_dimension as (
    -- Bảo đảm một video chỉ join một lần.
    select distinct on (nullif(trim(cast(v.video_id as text)), ''))
        nullif(trim(cast(v.video_id as text)), '') as video_id
        , v.video_name
        , v.video_url
        , v.channel_id
        , v.published_date
    from {{ ref('dim_video') }} v
    where nullif(trim(cast(v.video_id as text)), '') is not null
        and v.published_date is not null
        and nullif(trim(cast(v.editing_code as text)), '') is not null
    order by
        nullif(trim(cast(v.video_id as text)), '')
        , v.published_date desc nulls last
        , v.dim_video_sk
),

channel_dimension as (
    -- Bảo đảm một channel chỉ join một lần.
    select distinct on (nullif(trim(cast(c.channel_id as text)), ''))
        nullif(trim(cast(c.channel_id as text)), '') as channel_id
        , c.channel_name
    from {{ ref('dim_channel') }} c
    where nullif(trim(cast(c.channel_id as text)), '') is not null
    order by
        nullif(trim(cast(c.channel_id as text)), '')
        , c.dim_channel_sk
),

video_daily as (
    -- LEFT JOIN giữ metric chưa map được kho ở repository_id NULL.
    select
        rb.repository_id
        , f.video_id
        , cast(f.recorded_date as date) as recorded_date
        , sum(f.revenue_amount) as revenue_amount
        , sum(f."view") as views
    from {{ ref('fact_revenue_by_resources') }} f
    join video_dimension v
        on f.video_id = v.video_id
    left join resource_base rb
        on upper(trim(cast(f.resource_id as text))) = rb.resource_id
    where f.recorded_date is not null
    group by
        rb.repository_id
        , f.video_id
        , cast(f.recorded_date as date)
),

video_monthly as (
    select
        repository_id
        , video_id
        , date_trunc('month', recorded_date)::date as revenue_month
        , sum(views) as total_views
        , sum(revenue_amount) as total_revenue
    from video_daily
    group by
        repository_id
        , video_id
        , date_trunc('month', recorded_date)::date
),

video_range as (
    select
        repository_id
        , video_id
        , min(revenue_month) as min_m
        , max(revenue_month) as max_m
    from video_monthly
    group by repository_id, video_id
),

spine as (
    select
        r.repository_id
        , r.video_id
        , gs::date as revenue_month
    from video_range r
    cross join lateral generate_series(
        r.min_m,
        r.max_m,
        interval '1 month'
    ) as gs
),

spine_filled as (
    select
        s.repository_id
        , s.video_id
        , s.revenue_month
        , coalesce(m.total_views, 0) as total_views
        , coalesce(m.total_revenue, 0) as total_revenue
    from spine s
    left join video_monthly m
        on m.repository_id is not distinct from s.repository_id
        and m.video_id = s.video_id
        and m.revenue_month = s.revenue_month
),

with_growth as (
    select
        repository_id
        , video_id
        , revenue_month
        , total_views
        , total_revenue
        , lag(total_views) over (
            partition by repository_id, video_id
            order by revenue_month
        ) as prev_views
        , lag(total_revenue) over (
            partition by repository_id, video_id
            order by revenue_month
        ) as prev_revenue
    from spine_filled
)

select
    g.repository_id
    , v.video_id
    , v.video_name
    , v.video_url
    , c.channel_name
    , cast(v.published_date as date) as published_date
    , g.revenue_month
    , g.total_views as view_count
    , g.total_revenue as revenue_amount
    , case
        when g.prev_views is null or g.prev_views = 0 then null
        else (g.total_views - g.prev_views)::numeric / g.prev_views
      end as views_growth_pct
    , case
        when g.prev_revenue is null or g.prev_revenue = 0 then null
        else (g.total_revenue - g.prev_revenue)::numeric / g.prev_revenue
      end as revenue_growth_pct
from with_growth g
join video_dimension v
    on g.video_id = v.video_id
left join channel_dimension c
    on v.channel_id = c.channel_id
order by
    g.repository_id nulls last
    , c.channel_name
    , g.revenue_month desc
    , g.total_revenue desc
