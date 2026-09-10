-- silver.fact_editing
{{ config(
    materialized='incremental',
    unique_key='fact_editing_sk',
    incremental_strategy='delete+insert'
) }}

with excluded_stock as (
    select distinct
        upper(trim(cast(hg_stock_id as text))) as hg_stock_id
    from {{ ref('int_excluded_stock_codes') }}
    where nullif(trim(cast(hg_stock_id as text)), '') is not null
),
-- Deduplicate identical joined source rows before calculating position.
-- The unique test will still expose conflicting rows sharing the same Id.
source_rows as (
    select distinct
        re."Id" as resource_editing_id
        , re."EditingId" as editing_id
        , e."EditingFileId" as editing_code
        , upper(trim(cast(r."ResourceFileId" as text))) as hg_stock_id
        , re."StartTime" as start_time
        , re."EndTime" as end_time
    from {{ source('staging', 'resource_editings') }} re
    join {{ source('staging', 'editings') }} e
        on re."EditingId" = e."Id"
    join {{ source('staging', 'resource') }} r
        on re."ResourcesId" = r."Id"
    where r."ResourceType" = 0
            and upper(trim(cast(r."ResourceFileId" as text))) like 'HGFA%'
            and not exists (
                select 1
                from excluded_stock x
                where x.hg_stock_id
                    = upper(trim(cast(r."ResourceFileId" as text)))
    )
)

select
    {{ dbt_utils.generate_surrogate_key(['s.resource_editing_id']) }} as fact_editing_sk
    , s.editing_id
    , s.editing_code
    , s.hg_stock_id
    , row_number() over (
        partition by s.editing_id
        order by s.start_time, s.resource_editing_id
    ) as position
    , cast(s.end_time as numeric) - cast(s.start_time as numeric) as duration
from source_rows s
