{{ config(materialized='table') }}

-- Grain: one row per stock resource whose current dim_stock status is "Tồn kho".
-- Inventory age starts at dim_stock.stock_stored_date; resources without a valid
-- stored date remain in the mart and are assigned to a separate age bucket.

with inventory_stock as (
    select
        hg_stock_id
        , name
        , stock_link
        , stock_stored_date::date as inventory_start_date
        , status
    from {{ ref('dim_stock') }}
    where status = 'Tồn kho'
        and nullif(trim(hg_stock_id), '') is not null
),

-- Aggregate to the mart grain because dim_resources can contain duplicate HG IDs.
score_by_stock as (
    select
        hg_stock_id
        , max(acceptance_score) as acceptance_score
    from {{ ref('dim_resources') }}
    where nullif(trim(hg_stock_id), '') is not null
    group by hg_stock_id
),

base as (
    select
        st.hg_stock_id
        , st.name
        , st.stock_link
        , st.status
        , st.inventory_start_date
        , case
            when st.inventory_start_date is not null
                then (current_date - st.inventory_start_date)::int
          end as inventory_age_days
        , score.acceptance_score
    from inventory_stock st
    left join score_by_stock score
        on score.hg_stock_id = st.hg_stock_id
)

select
    hg_stock_id
    , name
    , stock_link
    , status
    , inventory_start_date
    , inventory_age_days
    , case
        when inventory_age_days between 0 and 30 then 1
        when inventory_age_days between 31 and 60 then 2
        when inventory_age_days > 60 then 3
        else 4
      end as inventory_age_sort
    , case
        when inventory_age_days between 0 and 30 then 'Tồn kho 0-30 ngày'
        when inventory_age_days between 31 and 60 then 'Tồn kho 30-60 ngày'
        when inventory_age_days > 60 then 'Tồn kho > 60 ngày'
        else 'Khác/Chưa có ngày'
      end as inventory_age_group
    , acceptance_score
    , case
        when acceptance_score >= 9.75 then 1
        when acceptance_score >= 9.25 then 2
        when acceptance_score >= 8.75 then 3
        when acceptance_score >= 8.25 then 4
        when acceptance_score >= 7.75 then 5
        else 6
      end as score_sort
    , case
        when acceptance_score >= 9.75 then '10 điểm'
        when acceptance_score >= 9.25 then '9.5 điểm'
        when acceptance_score >= 8.75 then '9 điểm'
        when acceptance_score >= 8.25 then '8.5 điểm'
        when acceptance_score >= 7.75 then '8 điểm'
        else 'Khác/Chưa có điểm'
      end as score_group
from base
