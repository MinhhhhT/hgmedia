{{ config(materialized='table') }}

-- Funnel hao hụt tài nguyên.
-- OD→SX dùng grain số lượng PO detail. Từ SX→NT trở đi dùng duy nhất
-- một grain: distinct HG stock ID của tài nguyên Odoo.
--
-- cohort_date và production project/repository được giữ cố định từ lúc tài
-- nguyên đi vào funnel; nhờ đó filter thời gian/dự án không đổi population
-- giữa các bước sau.

with repository_ctx as (
    select distinct on (r.repository_id)
        r.repository_id
        , r.sub_project_id
        , sp.project_id
    from {{ ref('dim_repository') }} r
    left join {{ ref('dim_sub_project') }} sp
        on sp.sub_project_id = r.sub_project_id
    where r.repository_id is not null
    order by r.repository_id, r.sub_project_id nulls last
),

po_line_ctx as (
    select
        pod.po_detail_id
        , pod.po_id
        , greatest(
            coalesce(nullif(trim(pod.qty_order::text), '')::numeric, 0)
            , 0
          ) as qty_order
        , nullif(trim(pod.repository), '') as repository_id
        , rc.sub_project_id as production_sub_project_id
        , rc.project_id as production_project_id
        , po.ordering_company as order_company_id
        , coalesce(po.po_confirmed_date, po.po_created_date)::date as cohort_date
    from {{ ref('fact_po_detail') }} pod
    left join {{ ref('dim_po') }} po
        on po.po_id = pod.po_id
    left join repository_ctx rc
        on rc.repository_id = nullif(trim(pod.repository), '')
),

-- dim_so.so_id hiện là sale_order_line.id. Vì vậy khóa đúng để nối một dòng
-- production là fact_so_detail.so_detail_id, không phải fact_so_detail.so_id.
production_by_po_detail as (
    select
        ds.po_id as po_detail_id
        , sum(
            greatest(
                coalesce(nullif(trim(fsd.production_qty::text), '')::numeric, 0)
                , 0
            )
          ) as production_qty
    from {{ ref('fact_so_detail') }} fsd
    inner join {{ ref('dim_so') }} ds
        on ds.so_id = fsd.so_detail_id
    where nullif(trim(ds.po_id), '') is not null
    group by ds.po_id
),

-- Ở grain PO detail, Hoàn thành không được vượt số lượng order.
od_sx_rows as (
    select
        1 as step_order
        , 'OD→SX' as buoc
        , v.status_order
        , v.tinh_trang
        , concat('OD_SX:', v.status_order, ':', pl.po_detail_id) as flow_item_key
        , cast(null as text) as resource_id
        , cast(null as text) as hg_stock_id
        , cast(null as text) as isrc
        , cast(null as text) as so_id
        , pl.po_id
        , pl.po_detail_id
        , cast(null as text) as channel_id
        , pl.repository_id
        , pl.production_sub_project_id
        , pl.production_project_id
        , cast(null as text) as stock_sub_project_id
        , cast(null as text) as stock_project_id
        , pl.production_sub_project_id as sub_project_id
        , pl.production_project_id as project_id
        , pl.order_company_id
        , cast(null as text) as stock_company_id
        , pl.order_company_id as company_id
        , cast(null as text) as net_id
        , cast(null as text) as platform
        , pl.cohort_date
        , cast(null as timestamp) as stock_stored_date
        , cast(null as timestamp) as recorded_date
        , v.so_luong::numeric as so_luong
    from po_line_ctx pl
    left join production_by_po_detail prod
        on prod.po_detail_id = pl.po_detail_id
    cross join lateral (
        values
            (
                1
                , 'Hoàn thành'
                , least(pl.qty_order, coalesce(prod.production_qty, 0))
            )
            , (
                2
                , 'Chưa hoàn thành'
                , greatest(pl.qty_order - coalesce(prod.production_qty, 0), 0)
            )
    ) v(status_order, tinh_trang, so_luong)
),

stock_by_hg as (
    select
        trim(hg_stock_id) as hg_stock_id
        , max(nullif(trim(isrc), '')) as isrc
        , min(stock_stored_date) as stock_stored_date
    from {{ ref('dim_stock') }}
    where nullif(trim(hg_stock_id), '') is not null
    group by trim(hg_stock_id)
),

-- Context theo SO header, dùng làm fallback khi resource chưa có po_detail_id.
so_header_ctx as (
    select
        fsd.so_id
        , min(coalesce(ds.so_confirmed_date, ds.so_created_date))::date as so_date
        , min(pod.po_id) as po_id
        , min(po.ordering_company) as order_company_id
    from {{ ref('fact_so_detail') }} fsd
    left join {{ ref('dim_so') }} ds
        on ds.so_id = fsd.so_detail_id
    left join {{ ref('fact_po_detail') }} pod
        on pod.po_detail_id = ds.po_id
    left join {{ ref('dim_po') }} po
        on po.po_id = pod.po_id
    where nullif(trim(fsd.so_id), '') is not null
    group by fsd.so_id
),

-- Một HG stock chỉ có một dòng trong funnel. Ưu tiên bản ghi đã nghiệm thu,
-- có repository, rồi bản ghi có acceptance_date mới nhất.
resource_ranked as (
    select
        trim(r.hg_stock_id) as hg_stock_id
        , coalesce(nullif(trim(r.odoo_id), ''), trim(r.hg_stock_id)) as resource_id
        , r.status
        , r.so_id
        , coalesce(pod.po_id, sh.po_id) as po_id
        , r.po_detail_id
        , r.production_plan_detail_id
        , r.repository_id
        , rc.sub_project_id as production_sub_project_id
        , rc.project_id as production_project_id
        , coalesce(po.ordering_company, sh.order_company_id) as order_company_id
        , sb.isrc
        , sb.stock_stored_date
        , coalesce(
            r.acceptance_date::date
            , po.po_confirmed_date::date
            , po.po_created_date::date
            , sh.so_date
          ) as cohort_date
        , row_number() over (
            partition by trim(r.hg_stock_id)
            order by
                case when r.status = 'Đã nghiệm thu' then 0 else 1 end
                , case when r.repository_id is not null then 0 else 1 end
                , r.acceptance_date desc nulls last
                , r.dim_resources_sk
          ) as resource_rank
    from {{ ref('dim_resources') }} r
    left join {{ ref('fact_po_detail') }} pod
        on pod.po_detail_id = r.po_detail_id
    left join {{ ref('dim_po') }} po
        on po.po_id = pod.po_id
    left join so_header_ctx sh
        on sh.so_id = r.so_id
    left join repository_ctx rc
        on rc.repository_id = r.repository_id
    left join stock_by_hg sb
        on sb.hg_stock_id = trim(r.hg_stock_id)
    where r.resource_source = 'odoo'
        and nullif(trim(r.hg_stock_id), '') is not null
),

resource_ctx as (
    select *
    from resource_ranked
    where resource_rank = 1
),

accepted_stock_ctx as (
    select *
    from resource_ctx
    where status = 'Đã nghiệm thu'
),

youtube_stock_ctx as (
    select distinct
        trim(y.hg_stock_id) as hg_stock_id
        , y.channel_id
        , ch.company_id as stock_company_id
        , ch.project_id as stock_project_id
        , ch.sub_project_id as stock_sub_project_id
        , coalesce(ch.network_id, y.net) as net_id
    from {{ ref('fact_youtube_operation') }} y
    left join {{ ref('dim_channel') }} ch
        on ch.channel_id = y.channel_id
    where nullif(trim(y.hg_stock_id), '') is not null
),

platform_by_stock as (
    select distinct
        trim(s.hg_stock_id) as hg_stock_id
        , nullif(trim(rd.platform), '') as platform
    from {{ ref('dim_stock') }} s
    inner join {{ ref('fact_revenue_distro') }} rd
        on rd.isrc = s.isrc
    where nullif(trim(s.hg_stock_id), '') is not null
        and nullif(trim(rd.platform), '') is not null

    union

    select distinct
        trim(s.hg_stock_id) as hg_stock_id
        , nullif(trim(rs.platform), '') as platform
    from {{ ref('dim_stock') }} s
    inner join {{ ref('fact_revenue_stream_distro') }} rs
        on rs.isrc = s.isrc
    where nullif(trim(s.hg_stock_id), '') is not null
        and nullif(trim(rs.platform), '') is not null
),

stock_filter_ctx as (
    select distinct on (coalesce(yt.hg_stock_id, pf.hg_stock_id))
        coalesce(yt.hg_stock_id, pf.hg_stock_id) as hg_stock_id
        , yt.channel_id
        , yt.stock_company_id
        , yt.stock_project_id
        , yt.stock_sub_project_id
        , yt.net_id
        , pf.platform
    from youtube_stock_ctx yt
    full join platform_by_stock pf
        on pf.hg_stock_id = yt.hg_stock_id
    where coalesce(yt.hg_stock_id, pf.hg_stock_id) is not null
    order by
        coalesce(yt.hg_stock_id, pf.hg_stock_id)
        , yt.channel_id nulls last
        , pf.platform nulls last
),

distributed_stock_ids as (
    select distinct trim(hg_stock_id) as hg_stock_id
    from {{ ref('fact_distribution') }}
    where nullif(trim(hg_stock_id), '') is not null
        and left(trim(hg_stock_id), 4) = 'HGFA'
),

published_video_editing_codes as (
    select distinct trim(editing_code) as editing_code
    from {{ ref('dim_video') }}
    where published_date is not null
        and nullif(trim(editing_code), '') is not null
),

used_stock_ids as (
    select distinct trim(fe.hg_stock_id) as hg_stock_id
    from {{ ref('fact_editing') }} fe
    inner join published_video_editing_codes pv
        on pv.editing_code = trim(fe.editing_code)
    where nullif(trim(fe.hg_stock_id), '') is not null
),

stock_result as (
    select
        trim(resource_id) as hg_stock_id
        , min(recorded_date) filter (
            where coalesce("view", 0) > 0 or coalesce(revenue_amount, 0) > 0
          ) as recorded_date
        , bool_or(coalesce("view", 0) > 0) as has_view
        , bool_or(coalesce(revenue_amount, 0) > 0) as has_revenue
    from {{ ref('fact_revenue_by_resources') }}
    where nullif(trim(resource_id), '') is not null
    group by trim(resource_id)
),

sx_nt_rows as (
    select
        2 as step_order
        , 'SX→NT' as buoc
        , case when r.status = 'Đã nghiệm thu' then 1 else 2 end as status_order
        , case when r.status = 'Đã nghiệm thu' then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat('SX_NT:', case when r.status = 'Đã nghiệm thu' then 1 else 2 end, ':', r.hg_stock_id) as flow_item_key
        , r.resource_id, r.hg_stock_id, r.isrc, r.so_id, r.po_id, r.po_detail_id
        , sf.channel_id, r.repository_id
        , r.production_sub_project_id, r.production_project_id
        , sf.stock_sub_project_id, sf.stock_project_id
        , r.production_sub_project_id as sub_project_id
        , r.production_project_id as project_id
        , r.order_company_id, sf.stock_company_id
        , coalesce(r.order_company_id, sf.stock_company_id) as company_id
        , sf.net_id, sf.platform, r.cohort_date, r.stock_stored_date
        , cast(null as timestamp) as recorded_date
        , 1::numeric as so_luong
    from resource_ctx r
    left join stock_filter_ctx sf
        on sf.hg_stock_id = r.hg_stock_id
),

nt_pp_rows as (
    select
        3 as step_order
        , 'NT→PP' as buoc
        , case when ds.hg_stock_id is not null then 1 else 2 end as status_order
        , case when ds.hg_stock_id is not null then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat('NT_PP:', case when ds.hg_stock_id is not null then 1 else 2 end, ':', r.hg_stock_id) as flow_item_key
        , r.resource_id, r.hg_stock_id, r.isrc, r.so_id, r.po_id, r.po_detail_id
        , sf.channel_id, r.repository_id
        , r.production_sub_project_id, r.production_project_id
        , sf.stock_sub_project_id, sf.stock_project_id
        , r.production_sub_project_id as sub_project_id
        , r.production_project_id as project_id
        , r.order_company_id, sf.stock_company_id
        , coalesce(r.order_company_id, sf.stock_company_id) as company_id
        , sf.net_id, sf.platform, r.cohort_date, r.stock_stored_date
        , cast(null as timestamp) as recorded_date
        , 1::numeric as so_luong
    from accepted_stock_ctx r
    left join distributed_stock_ids ds
        on ds.hg_stock_id = r.hg_stock_id
    left join stock_filter_ctx sf
        on sf.hg_stock_id = r.hg_stock_id
),

pp_sd_rows as (
    select
        4 as step_order
        , 'PP→SD' as buoc
        , case when us.hg_stock_id is not null then 1 else 2 end as status_order
        , case when us.hg_stock_id is not null then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat('PP_SD:', case when us.hg_stock_id is not null then 1 else 2 end, ':', r.hg_stock_id) as flow_item_key
        , r.resource_id, r.hg_stock_id, r.isrc, r.so_id, r.po_id, r.po_detail_id
        , sf.channel_id, r.repository_id
        , r.production_sub_project_id, r.production_project_id
        , sf.stock_sub_project_id, sf.stock_project_id
        , r.production_sub_project_id as sub_project_id
        , r.production_project_id as project_id
        , r.order_company_id, sf.stock_company_id
        , coalesce(r.order_company_id, sf.stock_company_id) as company_id
        , sf.net_id, sf.platform, r.cohort_date, r.stock_stored_date
        , cast(null as timestamp) as recorded_date
        , 1::numeric as so_luong
    from accepted_stock_ctx r
    inner join distributed_stock_ids ds
        on ds.hg_stock_id = r.hg_stock_id
    left join used_stock_ids us
        on us.hg_stock_id = r.hg_stock_id
    left join stock_filter_ctx sf
        on sf.hg_stock_id = r.hg_stock_id
),

-- Chỉ tài nguyên đã hoàn thành PP→SD mới được đi vào SD→KQ.
sd_kq_rows as (
    select
        5 as step_order
        , 'SD→KQ' as buoc
        , case when coalesce(sr.has_view, false) or coalesce(sr.has_revenue, false) then 1 else 2 end as status_order
        , case when coalesce(sr.has_view, false) or coalesce(sr.has_revenue, false) then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat('SD_KQ:', case when coalesce(sr.has_view, false) or coalesce(sr.has_revenue, false) then 1 else 2 end, ':', r.hg_stock_id) as flow_item_key
        , r.resource_id, r.hg_stock_id, r.isrc, r.so_id, r.po_id, r.po_detail_id
        , sf.channel_id, r.repository_id
        , r.production_sub_project_id, r.production_project_id
        , sf.stock_sub_project_id, sf.stock_project_id
        , r.production_sub_project_id as sub_project_id
        , r.production_project_id as project_id
        , r.order_company_id, sf.stock_company_id
        , coalesce(r.order_company_id, sf.stock_company_id) as company_id
        , sf.net_id, sf.platform, r.cohort_date, r.stock_stored_date, sr.recorded_date
        , 1::numeric as so_luong
    from accepted_stock_ctx r
    inner join distributed_stock_ids ds
        on ds.hg_stock_id = r.hg_stock_id
    inner join used_stock_ids us
        on us.hg_stock_id = r.hg_stock_id
    left join stock_result sr
        on sr.hg_stock_id = r.hg_stock_id
    left join stock_filter_ctx sf
        on sf.hg_stock_id = r.hg_stock_id
),

flow_rows as (
    select * from od_sx_rows
    union all select * from sx_nt_rows
    union all select * from nt_pp_rows
    union all select * from pp_sd_rows
    union all select * from sd_kq_rows
)

select
    step_order, buoc, status_order, tinh_trang, flow_item_key
    , resource_id, hg_stock_id, isrc, so_id, po_id, po_detail_id, channel_id
    , repository_id, production_sub_project_id, production_project_id
    , stock_sub_project_id, stock_project_id, sub_project_id, project_id
    , order_company_id, stock_company_id, company_id, net_id, platform
    , cohort_date, stock_stored_date, recorded_date, so_luong
from flow_rows
where coalesce(so_luong, 0) > 0
order by step_order, status_order
