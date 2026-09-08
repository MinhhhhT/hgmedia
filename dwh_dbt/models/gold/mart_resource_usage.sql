{{ config(materialized='table') }}

-- Mart resource usage.
-- Grain: 1 HG stock ID. dim_stock là bảng gốc để giữ mọi HG stock hợp lệ.

with resource_base as (

    select distinct on (r.hg_stock_id)
        r.hg_stock_id as resource_id
        , r.resource_name
        , r.resource_source as resource_origin
        , r.repository_type as source
        , r.repository_id
        , r.production_plan_detail_id
    from {{ ref('dim_resources') }} r
    where nullif(trim(r.hg_stock_id), '') is not null
    order by
        r.hg_stock_id
        , case when r.repository_id is not null then 0 else 1 end
        , case when r.odoo_id is not null then 0 else 1 end
        , r.acceptance_date desc nulls last
        , r.dim_resources_sk

),

resource_hierarchy as (

    select
        rb.resource_id
        , rb.resource_name
        , rb.resource_origin
        , rb.source
        , rb.repository_id
        , repo.repository_name as repository
        , sp.sub_project_id
        , sp.sub_project_name
        , p.project_id
        , p.project_name as project
        , rb.production_plan_detail_id
    from resource_base rb
    left join {{ ref('dim_repository') }} repo
        on rb.repository_id = repo.repository_id
    left join {{ ref('dim_sub_project') }} sp
        on repo.sub_project_id = sp.sub_project_id
    left join {{ ref('dim_project') }} p
        on sp.project_id = p.project_id

),

-- Bảng gốc: mọi HG stock hợp lệ trong dim_stock.
stock_info as (

    select distinct on (trim(hg_stock_id))
        trim(hg_stock_id) as resource_id
        , name as stock_name
        , isrc
        , stock_stored_date
        , status as stock_status
    from {{ ref('dim_stock') }}
    where nullif(trim(hg_stock_id), '') is not null
    order by
        trim(hg_stock_id)
        , stock_stored_date desc nulls last
        , dim_stock_sk

),

distribution_team as (

    select
        hg_stock_id as resource_id
        , string_agg(
            distinct nullif(trim(team), ''),
            ', '
          ) as distribution_team
        , max(distribution_date)::date as last_distribution_date
    from {{ ref('fact_distribution') }}
    where nullif(trim(hg_stock_id), '') is not null
    group by hg_stock_id

),

ar_by_resource as (

    select
        rh.resource_id
        , string_agg(
            distinct nullif(trim(ar."tên a&r"), ''),
            ', '
          ) as ar_name
    from resource_hierarchy rh
    left join {{ ref('dim_ar') }} ar
        on rh.production_plan_detail_id = ar."mã a&r"
    group by rh.resource_id

),

seo_by_resource as (

    select
        r.hg_stock_id as resource_id
        , string_agg(
            distinct xms.employee_id::bigint::text,
            ', '
          ) as seo_employee_id
        , string_agg(
            distinct nullif(trim(he.name), ''),
            ', '
          ) as seo_name
    from {{ ref('dim_resources') }} r
    inner join {{ source('staging', 'x_music_song') }} xms
        on nullif(trim(r.odoo_id), '') = xms.id::text
    left join {{ source('staging', 'hr_employee') }} he
        on he.id = xms.employee_id::bigint
    where nullif(trim(r.hg_stock_id), '') is not null
        and xms.employee_id is not null
    group by r.hg_stock_id

),

video_usage as (

    select
        fe.hg_stock_id as resource_id
        , count(distinct b.video_id) as used_video_count
        , max(dv.published_date)::date as last_used_published_date
    from {{ ref('fact_editing') }} fe
    inner join {{ ref('bridge_bt_vid') }} b
        on fe.editing_code = b.editing_code
    left join {{ ref('dim_video') }} dv
        on b.video_id = dv.video_id
    where nullif(trim(fe.hg_stock_id), '') is not null
        and nullif(trim(fe.editing_code), '') is not null
        and nullif(trim(b.video_id), '') is not null
    group by fe.hg_stock_id

)

select
    {{ dbt_utils.generate_surrogate_key(['si.resource_id']) }}
        as mart_resource_usage_sk

    , si.resource_id
    , rh.resource_name
    , si.stock_name
    , si.isrc

    , rh.resource_origin
    , rh.source

    , rh.repository
    , rh.repository_id

    , rh.sub_project_name
    , rh.sub_project_id

    , rh.project
    , rh.project_id

    , si.stock_status
    , si.stock_stored_date::date as stock_stored_date

    -- Tuổi tài nguyên: ngày từ lúc nhập kho đến hiện tại.
    , case
        when si.stock_stored_date is not null
            then current_date - si.stock_stored_date::date
      end as resource_age_days

    , dt.distribution_team
    , dt.last_distribution_date

    , ar.ar_name

    , seo.seo_employee_id
    , seo.seo_name

    , coalesce(vu.used_video_count, 0) as used_video_count
    , vu.last_used_published_date

    -- Áp dụng cho TN Hàng nguội / Sử dụng.
    , case
        when si.stock_status in ('Hàng nguội', 'Sử dụng')
            and vu.last_used_published_date is not null
            then current_date - vu.last_used_published_date
      end as days_since_last_usage

    -- Áp dụng cho TN Tồn kho, chưa có lượt sử dụng đầu tiên.
    , case
        when si.stock_status = 'Tồn kho'
            and si.stock_stored_date is not null
            then current_date - si.stock_stored_date::date
      end as inventory_unused_days

from stock_info si
left join resource_hierarchy rh
    on si.resource_id = rh.resource_id
left join distribution_team dt
    on si.resource_id = dt.resource_id
left join ar_by_resource ar
    on si.resource_id = ar.resource_id
left join seo_by_resource seo
    on si.resource_id = seo.resource_id
left join video_usage vu
    on si.resource_id = vu.resource_id

