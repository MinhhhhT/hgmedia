with expected as (
    select
        f.video_id
        , date_trunc('month', f.recorded_date)::date as revenue_month
        , nullif(sum(f."view"), 0) as view_count
        , nullif(sum(f.revenue_amount), 0) as revenue_amount
    from {{ ref('fact_revenue_by_resources') }} f
    where f.video_id is not null
        and f.recorded_date is not null
    group by
        f.video_id
        , date_trunc('month', f.recorded_date)::date
)

select
    actual.repository_id
    , actual.video_id
    , actual.revenue_month
    , actual.view_count as actual_view_count
    , expected.view_count as expected_view_count
    , actual.revenue_amount as actual_revenue_amount
    , expected.revenue_amount as expected_revenue_amount
from {{ ref('mart_video_performance_monthly') }} actual
left join expected
    on actual.video_id = expected.video_id
    and actual.revenue_month = expected.revenue_month
where actual.view_count is distinct from expected.view_count
    or actual.revenue_amount is distinct from expected.revenue_amount
