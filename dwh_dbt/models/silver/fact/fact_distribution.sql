-- silver.fact_distribution
{{ config(
    materialized='incremental',
    unique_key='fact_distribution_sk',
    incremental_strategy='delete+insert'
) }}

-- Optimized to hash-join the exclusion set once instead of executing
-- a correlated NOT EXISTS scan for every distribution row.
with excluded_stock as (
    select distinct
        upper(trim(cast(hg_stock_id as text))) as hg_stock_id
    from {{ ref('int_excluded_stock_codes') }}
    where nullif(trim(cast(hg_stock_id as text)), '') is not null
),

filtered_distribution as (
    select
        d."Id"
        , d."CreatedDate"
        , d."CreatedByUserId"
        , d."DepartmentData"
        , d."GroupData"
        , d."UserData"
        , upper(trim(cast(rfi."ResourceFileId" as text))) as hg_stock_id
    from {{ source('staging','distribution_media_history') }} d
    left join {{ source('staging','resource_file_info') }} rfi
            on d."ResourceFileInfoId" = rfi."Id"
            and upper(trim(cast(rfi."ResourceFileId" as text))) like 'HGFA%'
    left join excluded_stock x
            on x.hg_stock_id
            = upper(trim(cast(rfi."ResourceFileId" as text)))
    where x.hg_stock_id is null
),

base as (
    select
        {{ dbt_utils.generate_surrogate_key(['d."Id"']) }} as fact_distribution_sk
        , d."Id" as distribution_id
        , d.hg_stock_id
        , cast(d."CreatedDate" as timestamp) as distribution_date
        , 'Dùng chung' as recipient_company
        , dep.dept_names as department
        , grp.team_names as team
        , usr.emp_names as employee
        , d."CreatedByUserId" as distributed_employee_id
    from filtered_distribution d

    left join lateral (
        select string_agg(dd."Name", ', ') as dept_names
        from unnest(string_to_array(nullif(d."DepartmentData",''), ',')) g(id)
        join {{ source('staging','department') }} dd
            on trim(g.id) = dd."Id"::text
    ) dep on true

    left join lateral (
        select string_agg(gg."Name", ', ') as team_names
        from unnest(string_to_array(nullif(d."GroupData",''), ',')) g(id)
        join {{ source('staging','groups') }} gg
            on trim(g.id) = gg."Id"::text
    ) grp on true

    left join lateral (
        select string_agg(uu."DisplayName", ', ') as emp_names
        from unnest(string_to_array(nullif(d."UserData",''), ',')) g(id)
        join {{ source('staging','users') }} uu
            on trim(g.id) = uu."Id"::text
    ) usr on true
)

select *
from base
