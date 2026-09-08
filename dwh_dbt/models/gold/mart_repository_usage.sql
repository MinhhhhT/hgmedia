{{ config(
    materialized='table',
    schema='gold'
) }}

with resource_base as (

    select
        trim(cast(dr.hg_stock_id as text)) as hg_stock_id
        , cast(dr.repository_id as text) as repository_id
        , dr.acceptance_cost
        , dr.acceptance_score
    from {{ ref('dim_resources') }} dr

),

repository_dimension as (

    select
        cast(repo.repository_id as text) as repository_id
        , repo.repository_name
        , cast(dsp.sub_project_id as text) as sub_project_id
        , dsp.sub_project_name
        , cast(dp.project_id as text) as project_id
        , dp.project_name
    from {{ ref('dim_repository') }} repo
    left join {{ ref('dim_sub_project') }} dsp
        on cast(repo.sub_project_id as text) = cast(dsp.sub_project_id as text)
    left join {{ ref('dim_project') }} dp
        on cast(dsp.project_id as text) = cast(dp.project_id as text)

),

-- Dùng cho các metric cần danh sách mã tài nguyên duy nhất theo Kho.
scope_stock as (

    select distinct
        repository_id
        , hg_stock_id
    from resource_base
    where hg_stock_id is not null
      and hg_stock_id <> ''

),

resource_metrics as (

    select
        repository_id

        -- Giữ logic loại #N/A, #REF!, Không tìm thấy.
        , count(
            distinct case
                when upper(coalesce(hg_stock_id, '')) not in (
                    '#N/A',
                    '#REF!',
                    'KHÔNG TÌM THẤY'
                )
                    then coalesce(hg_stock_id, '__BLANK_HG_STOCK_ID__')
            end
        ) as number_resources

        -- Tương ứng SUM(dim_resources[acceptance_cost]).
        , sum(acceptance_cost) as standard_cost

        -- Tương ứng AVERAGE(score) với score <> 0.
        , avg(acceptance_score) filter (
            where acceptance_score <> 0
        ) as acceptance_score

    from resource_base
    group by repository_id

),

revenue_metrics as (

    select
        ss.repository_id
        , sum(fr.revenue_amount) as revenue
    from scope_stock ss
    inner join {{ ref('fact_revenue_by_resources') }} fr
        on ss.hg_stock_id = trim(cast(fr.resource_id as text))
    group by ss.repository_id

),

purchase_cost_metrics as (

    select
        ss.repository_id
        , sum(fpc.additional_cost) as purchase_cost
    from scope_stock ss
    inner join {{ ref('fact_purchase_cost') }} fpc
        on ss.hg_stock_id = trim(cast(fpc.resource_id as text))
    group by ss.repository_id

),

-- Một video có thể có nhiều editing_code.
published_video_editing as (

    select distinct
        cast(dv.video_id as text) as video_id
        , trim(cast(dv.editing_code as text)) as editing_code
    from {{ ref('dim_video') }} dv
    where dv.published_date is not null
      and dv.video_id is not null
      and dv.editing_code is not null

),

-- Mỗi tài nguyên trong một video chỉ còn một dòng;
-- ưu tiên vị trí nhỏ nhất nếu tài nguyên xuất hiện nhiều lần.
resource_video_usage as (

    select
        ss.repository_id
        , pve.video_id
        , ss.hg_stock_id
        , min(fe.position) as min_position
    from published_video_editing pve
    inner join {{ ref('fact_editing') }} fe
        on trim(cast(fe.editing_code as text)) = pve.editing_code
    inner join scope_stock ss
        on trim(cast(fe.hg_stock_id as text)) = ss.hg_stock_id
    where fe.hg_stock_id is not null
      and fe.position is not null
    group by
        ss.repository_id
        , pve.video_id
        , ss.hg_stock_id

),

usage_metrics as (

    select
        repository_id

        -- Số tài nguyên có ít nhất một lần được dùng trong video published.
        , count(distinct hg_stock_id) as used_resources

        -- Tổng số cặp tài nguyên × video published.
        , count(*) as total_resource_usage_published

        -- Các nhóm loại trừ nhau do đã lấy min_position.
        , count(*) filter (
            where min_position = 1
        ) as usage_position_1

        , count(*) filter (
            where min_position between 2 and 3
        ) as usage_position_2_3

        , count(*) filter (
            where min_position between 4 and 5
        ) as usage_position_4_5

        , count(*) filter (
            where min_position > 5
        ) as usage_position_gt_5

    from resource_video_usage
    group by repository_id

)

select
    {{ dbt_utils.generate_surrogate_key(['rd.repository_id']) }}
        as mart_repository_resource_performance_sk

    , rd.project_id
    , rd.project_name as project
    , rd.sub_project_id
    , rd.sub_project_name as sub_project
    , rd.repository_id
    , rd.repository_name as repository

    , coalesce(rm.number_resources, 0) as number_resources
    , coalesce(um.used_resources, 0) as used_resources

    , coalesce(rev.revenue, 0) as revenue

    , coalesce(rm.standard_cost, 0) as standard_cost
    , coalesce(pc.purchase_cost, 0) as purchase_cost
    , coalesce(rm.standard_cost, 0) + coalesce(pc.purchase_cost, 0)
        as total_cost

    , rm.acceptance_score

    , coalesce(um.total_resource_usage_published, 0)
        as total_resource_usage_published
    , coalesce(um.usage_position_1, 0) as usage_position_1
    , coalesce(um.usage_position_2_3, 0) as usage_position_2_3
    , coalesce(um.usage_position_4_5, 0) as usage_position_4_5
    , coalesce(um.usage_position_gt_5, 0) as usage_position_gt_5

    , coalesce(
        um.usage_position_1::numeric
        / nullif(um.total_resource_usage_published, 0),
        0
    ) as usage_position_1_pct

    , coalesce(
        um.usage_position_2_3::numeric
        / nullif(um.total_resource_usage_published, 0),
        0
    ) as usage_position_2_3_pct

    , coalesce(
        um.usage_position_4_5::numeric
        / nullif(um.total_resource_usage_published, 0),
        0
    ) as usage_position_4_5_pct

    , coalesce(
        um.usage_position_gt_5::numeric
        / nullif(um.total_resource_usage_published, 0),
        0
    ) as usage_position_gt_5_pct

from repository_dimension rd
left join resource_metrics rm
    on rd.repository_id = rm.repository_id
left join revenue_metrics rev
    on rd.repository_id = rev.repository_id
left join purchase_cost_metrics pc
    on rd.repository_id = pc.repository_id
left join usage_metrics um
    on rd.repository_id = um.repository_id