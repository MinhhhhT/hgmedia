{{ config(materialized='table') }}

-- Grain: one row per ISRC from staging.sale.
-- Descriptive attributes come from the latest reporting period in sale.

with sale_ranked as (

    select
        nullif(trim(cast(s.isrc as text)), '') as isrc
        , nullif(trim(cast(s.label as text)), '') as label
        , nullif(trim(cast(s.distro as text)), '') as distro
        , nullif(trim(cast(s.song as text)), '') as song
        , row_number() over (
            partition by nullif(trim(cast(s.isrc as text)), '')
            order by
                nullif(trim(cast(s."reportingPeriod" as text)), '')::date desc nulls last
                , s._loaded_at desc nulls last
                , s._source_id desc nulls last
                , s.retailer desc nulls last
          ) as row_num
    from {{ source('staging', 'sale') }} s
    where nullif(trim(cast(s.isrc as text)), '') is not null

),

latest_sale as (

    select
        isrc
        , label
        , distro
        , song
    from sale_ranked
    where row_num = 1

),

x_music_song_ranked as (

    select
        nullif(trim(cast(xms.isrc as text)), '') as isrc
        , case
            when xms.subgenre_id is not null
                then xms.subgenre_id::bigint::text
          end as repository_id
        , row_number() over (
            partition by nullif(trim(cast(xms.isrc as text)), '')
            order by
                xms.write_date desc nulls last
                , xms.id desc
          ) as row_num
    from {{ source('staging', 'x_music_song') }} xms
    where nullif(trim(cast(xms.isrc as text)), '') is not null

),

x_music_song_by_isrc as (

    select
        xms.isrc
        , r.repository_id
    from x_music_song_ranked xms
    left join {{ ref('dim_repository') }} r
        on r.repository_id = xms.repository_id
    where xms.row_num = 1

),

distro_information_ranked as (

    select
        nullif(trim(cast(d.isrc as text)), '') as isrc
        , nullif(
            regexp_replace(
                normalize(trim(cast(d."Dự án" as text)), nfc),
                '\s+', ' ', 'g'
            ),
            ''
          ) as project_name
        , nullif(
            regexp_replace(
                normalize(trim(cast(d."Dự án con" as text)), nfc),
                '\s+', ' ', 'g'
            ),
            ''
          ) as sub_project_name
        , nullif(
            regexp_replace(
                normalize(trim(cast(d."Kho" as text)), nfc),
                '\s+', ' ', 'g'
            ),
            ''
          ) as repository_name
        , row_number() over (
            partition by nullif(trim(cast(d.isrc as text)), '')
            order by
                d._loaded_at desc nulls last
                , d._source_id desc nulls last
          ) as row_num
    from {{ source('staging', 'distro_infomation') }} d
    where nullif(trim(cast(d.isrc as text)), '') is not null

),

latest_distro_information as (

    select
        isrc
        , project_name
        , sub_project_name
        , repository_name
    from distro_information_ranked
    where row_num = 1

),

distro_repository_candidates as (

    select
        d.isrc
        , r.repository_id
        , row_number() over (
            partition by d.isrc
            order by
                case when r.repository_id ~ '^[0-9]+$' then 0 else 1 end
                , case
                    when r.repository_id ~ '^[0-9]+$'
                        then r.repository_id::numeric
                  end nulls last
                , r.repository_id
          ) as row_num
    from latest_distro_information d
    left join {{ ref('dim_project') }} p
        on lower(
            regexp_replace(normalize(trim(p.project_name), nfc), '\s+', ' ', 'g')
        ) = lower(d.project_name)
    left join {{ ref('dim_sub_project') }} sp
        on sp.project_id = p.project_id
        and lower(
            regexp_replace(normalize(trim(sp.sub_project_name), nfc), '\s+', ' ', 'g')
        ) = lower(d.sub_project_name)
    left join {{ ref('dim_repository') }} r
        on r.sub_project_id = sp.sub_project_id
        and lower(
            regexp_replace(normalize(trim(r.repository_name), nfc), '\s+', ' ', 'g')
        ) = lower(coalesce(d.repository_name, 'Không có kho'))

),

repository_by_distro as (

    select
        isrc
        , repository_id
    from distro_repository_candidates
    where row_num = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['s.isrc']) }} as dim_isrc_sk
    , s.isrc
    , s.label
    , s.distro
    , s.song
    , coalesce(xms.repository_id, d.repository_id) as repository_id
from latest_sale s
left join x_music_song_by_isrc xms
    on xms.isrc = s.isrc
left join repository_by_distro d
    on d.isrc = s.isrc
