{{ config(
    materialized='table',
    schema='gold'
) }}

-- Mart: hiệu quả tài nguyên theo kênh.
-- Grain: 1 hg_stock_id x 1 record_date.
-- record_date tạo quan hệ trực tiếp với dim_date_kt để View/Revenue phản ánh
-- đúng khoảng ngày được chọn trong Power BI.

with stock_resource_bridge as (

    select distinct on (trim(cast(id as text)))
        trim(cast(hg_code as text)) as hg_stock_id
        , trim(cast(id as text)) as source_resource_id
    from {{ source('staging', 'x_music_song') }}
    where hg_code is not null
    order by
        trim(cast(id as text))
        , trim(cast(hg_code as text))

),

-- Map chuẩn theo HG stock. Tài nguyên thu mua được ưu tiên bản ghi
-- dim_resources có resource_source = purchased để luôn lấy đúng repository.
stock_resource_ctx as (

    select distinct on (trim(cast(hg_stock_id as text)))
        trim(cast(hg_stock_id as text)) as hg_stock_id
        , nullif(trim(cast(odoo_id as text)), '') as source_resource_id
        , resource_name
        , repository_id
        , acceptance_score
        , resource_source
    from {{ ref('dim_resources') }}
    where nullif(trim(cast(hg_stock_id as text)), '') is not null
    order by
        trim(cast(hg_stock_id as text))
        , case when resource_source = 'purchased' then 0 else 1 end
        , acceptance_date desc nulls last
        , odoo_id nulls last

),

-- Fallback cho stock sản xuất cũ chưa có map trực tiếp trong dim_resources.
legacy_resource_ctx as (

    select
        srb.hg_stock_id
        , dr.resource_name
        , dr.repository_id
        , dr.acceptance_score
    from stock_resource_bridge srb
    inner join {{ ref('dim_resource') }} dr
        on srb.source_resource_id = trim(cast(dr.resource_id as text))

),

purchased_stock_ctx as (

    select distinct
        trim(cast(hg_stock_id as text)) as hg_stock_id
    from {{ ref('dim_purchased_resource') }}
    where nullif(trim(cast(hg_stock_id as text)), '') is not null

),

-- Chuẩn hóa resource_id từ fact thành hg_stock_id.
-- Nếu resource_id đã là HG code thì giữ nguyên.
fact_revenue_by_stock as (

    select
        coalesce(
            srb.hg_stock_id,
            trim(cast(f.resource_id as text))
        ) as hg_stock_id
        , cast(f.recorded_date as date) as record_date
        , f."view" as view
        , f.revenue_amount
    from {{ ref('fact_revenue_by_resources') }} f
    left join stock_resource_bridge srb
        on trim(cast(f.resource_id as text)) = srb.source_resource_id
    where f.resource_id is not null

),

-- Không tổng hợp toàn thời gian: giữ record_date để dim_date_kt lọc trực tiếp.
daily_stock_view_revenue as (

    select
        hg_stock_id
        , record_date
        , sum(view) as view
        , sum(revenue_amount) as revenue
    from fact_revenue_by_stock
    where hg_stock_id is not null
        and record_date is not null
    group by
        hg_stock_id
        , record_date

),

monthly_metrics as (

    select
        hg_stock_id
        , date_trunc('month', record_date)::date as month
        , sum(view) as monthly_view
        , sum(revenue) as monthly_revenue
    from daily_stock_view_revenue
    group by
        hg_stock_id
        , date_trunc('month', record_date)::date

),

growth as (

    select
        hg_stock_id
        , month
        , (
            monthly_view
            - lag(monthly_view) over (
                partition by hg_stock_id
                order by month
            )
        )::numeric
        / nullif(
            lag(monthly_view) over (
                partition by hg_stock_id
                order by month
            ),
            0
        ) as growth_view
        , (
            monthly_revenue
            - lag(monthly_revenue) over (
                partition by hg_stock_id
                order by month
            )
        )::numeric
        / nullif(
            lag(monthly_revenue) over (
                partition by hg_stock_id
                order by month
            ),
            0
        ) as growth_revenue
    from monthly_metrics

),

channel_video as (

    select
        trim(cast(sv.hg_stock_id as text)) as hg_stock_id
        , count(distinct dv.channel_id) as channel_use
        , count(distinct sv.video_id) as video_use
    from {{ ref('int_stock_video') }} sv
    inner join {{ ref('dim_video') }} dv
        on cast(sv.video_id as text) = cast(dv.video_id as text)
    where dv.published_date is not null
    group by trim(cast(sv.hg_stock_id as text))

)

select
    {{ dbt_utils.generate_surrogate_key([
        'ds.hg_stock_id',
        'dsvr.record_date'
    ]) }} as mart_resource_channel_sk

    , trim(cast(ds.hg_stock_id as text)) as resource_id
    , dsvr.record_date
    , coalesce(src.resource_name, lrc.resource_name, ds.name) as resource_name

    , dp.project_name as project
    , dsp.sub_project_name as sub_project

    , coalesce(src.acceptance_score, lrc.acceptance_score) as acceptance_score

    , case
        when pr.hg_stock_id is not null
            or src.resource_source = 'purchased'
            then 'Thu mua'
        else 'Sản xuất'
    end as resource_type

    , coalesce(dsvr.view, 0) as view
    , coalesce(dsvr.revenue, 0) as revenue

    , g.growth_view
    , g.growth_revenue

    , coalesce(cv.channel_use, 0) as channel_use
    , coalesce(cv.video_use, 0) as video_use

from {{ ref('dim_stock') }} ds

left join daily_stock_view_revenue dsvr
    on trim(cast(ds.hg_stock_id as text)) = dsvr.hg_stock_id

-- Ưu tiên dim_resources; legacy map chỉ dùng khi không có bản ghi trực tiếp.
left join stock_resource_ctx src
    on trim(cast(ds.hg_stock_id as text)) = src.hg_stock_id

left join legacy_resource_ctx lrc
    on trim(cast(ds.hg_stock_id as text)) = lrc.hg_stock_id
    and src.hg_stock_id is null

left join {{ ref('dim_repository') }} repo
    on cast(coalesce(src.repository_id, lrc.repository_id) as text)
        = cast(repo.repository_id as text)

left join {{ ref('dim_sub_project') }} dsp
    on cast(repo.sub_project_id as text) = cast(dsp.sub_project_id as text)

left join {{ ref('dim_project') }} dp
    on cast(dsp.project_id as text) = cast(dp.project_id as text)

left join purchased_stock_ctx pr
    on trim(cast(ds.hg_stock_id as text)) = pr.hg_stock_id

left join growth g
    on trim(cast(ds.hg_stock_id as text)) = g.hg_stock_id
    and date_trunc('month', dsvr.record_date)::date = g.month

left join channel_video cv
    on trim(cast(ds.hg_stock_id as text)) = cv.hg_stock_id
