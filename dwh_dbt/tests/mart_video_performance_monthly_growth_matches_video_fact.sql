with actual_months as (
    select distinct
        video_id
        , revenue_month
    from {{ ref('mart_video_performance_monthly') }}
),

fact_monthly as (
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
),

monthly as (
    select
        m.video_id
        , m.revenue_month
        , f.view_count
        , f.revenue_amount
    from actual_months m
    left join fact_monthly f
        on m.video_id = f.video_id
        and m.revenue_month = f.revenue_month
),

with_previous as (
    select
        video_id
        , revenue_month
        , view_count
        , revenue_amount
        , lag(view_count) over (
            partition by video_id order by revenue_month
        ) as previous_view_count
        , lag(revenue_amount) over (
            partition by video_id order by revenue_month
        ) as previous_revenue_amount
    from monthly
),

expected as (
    select
        video_id
        , revenue_month
        , case
            when view_count is null or previous_view_count is null then null
            else (view_count - previous_view_count)::numeric
                / previous_view_count
          end as views_growth_pct
        , case
            when revenue_amount is null or previous_revenue_amount is null then null
            else (revenue_amount - previous_revenue_amount)::numeric
                / previous_revenue_amount
          end as revenue_growth_pct
    from with_previous
)

select
    actual.repository_id
    , actual.video_id
    , actual.revenue_month
    , actual.views_growth_pct as actual_views_growth_pct
    , expected.views_growth_pct as expected_views_growth_pct
    , actual.revenue_growth_pct as actual_revenue_growth_pct
    , expected.revenue_growth_pct as expected_revenue_growth_pct
from {{ ref('mart_video_performance_monthly') }} actual
join expected
    on actual.video_id = expected.video_id
    and actual.revenue_month = expected.revenue_month
where actual.views_growth_pct is distinct from expected.views_growth_pct
    or actual.revenue_growth_pct is distinct from expected.revenue_growth_pct
