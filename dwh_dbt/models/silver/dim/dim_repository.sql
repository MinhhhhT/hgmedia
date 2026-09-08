-- Replacement model for dim_repository.
-- Repository names are stored as NFC, trimmed and with repeated whitespace collapsed.
-- When duplicate names exist under one canonical sub-project, numeric IDs are retained.

with canonical_projects as (
    select
        dp.project_id as canonical_project_id
        , nullif(regexp_replace(normalize(trim(dp.project_name), nfc), '\s+', ' ', 'g'), '') as project_name
    from {{ ref('dim_project') }} dp
    where dp.project_id is not null
        and nullif(trim(dp.project_name), '') is not null
),

canonical_sub_projects as (
    select distinct on (
        dsp.project_id,
        lower(normalize(trim(dsp.sub_project_name), nfc))
    )
        dsp.sub_project_id as source_sub_project_id
        , dsp.sub_project_id as canonical_sub_project_id
        , dsp.project_id as canonical_project_id
        , nullif(regexp_replace(normalize(trim(dsp.sub_project_name), nfc), '\s+', ' ', 'g'), '') as sub_project_name
    from {{ ref('dim_sub_project') }} dsp
    where dsp.sub_project_id is not null
        and dsp.project_id is not null
        and nullif(trim(dsp.sub_project_name), '') is not null
    order by
        dsp.project_id
        , lower(normalize(trim(dsp.sub_project_name), nfc))
        , case when dsp.sub_project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when dsp.sub_project_id ~ '^[0-9]+$' then dsp.sub_project_id::numeric end nulls last
        , dsp.sub_project_id
),

from_odoo as (
    select
        nullif(trim(cast(xps.id as text)), '') as repository_id
        , coalesce(
            csp.canonical_sub_project_id,
            nullif(trim(cast(xps.product_genre_id as text)), '')
        ) as sub_project_id
        , nullif(regexp_replace(normalize(trim(cast(xps.name as text)), nfc), '\s+', ' ', 'g'), '') as repository_name
        , 1 as source_priority
    from {{ source('staging', 'x_product_subgenre') }} xps
    left join canonical_sub_projects csp
        on nullif(trim(cast(xps.product_genre_id as text)), '')
            = csp.source_sub_project_id
    where nullif(trim(cast(xps.product_genre_id as text)), '') is not null
        and xps.order_type = 'music'
),

from_partners as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'p."Tên đối tác"',
            'cast(csp.canonical_sub_project_id as text)'
        ]) }} as repository_id
        , cast(csp.canonical_sub_project_id as text) as sub_project_id
        , nullif(regexp_replace(normalize(trim(p."Tên đối tác"), nfc), '\s+', ' ', 'g'), '') as repository_name
        , 2 as source_priority
    from {{ source('staging', 'partners') }} p
    inner join canonical_projects cp
        on lower(normalize(trim(p."Dự án"), nfc))
            = lower(normalize(cp.project_name, nfc))
    inner join canonical_sub_projects csp
        on csp.canonical_project_id = cp.canonical_project_id
        and lower(normalize(csp.sub_project_name, nfc))
            = lower(normalize('Không có dự án con', nfc))
    where nullif(trim(p."Tên đối tác"), '') is not null
        and not exists (
            select 1
            from from_odoo fo
            where fo.sub_project_id = cast(csp.canonical_sub_project_id as text)
                and lower(normalize(fo.repository_name, nfc))
                    = lower(normalize(trim(p."Tên đối tác"), nfc))
        )
),

performance_clean as (
    select distinct
        case
            when nullif(trim(p."Kho (nếu có)"), '') is null
                or upper(trim(p."Kho (nếu có)")) = '#N/A'
                then 'Không có kho'
            when lower(normalize(trim(p."Kho (nếu có)"), nfc)) in (
                'audiojungle',
                'nhạc nền audiojungle',
                'nhạc nền tảng audiojungle',
                'nền tảng audiojungle'
            ) then 'Audiojungle'
            else nullif(regexp_replace(normalize(trim(p."Kho (nếu có)"), nfc), '\s+', ' ', 'g'), '')
          end as repository_name
        , nullif(regexp_replace(normalize(trim(p."Dự án chốt"), nfc), '\s+', ' ', 'g'), '') as project_name
        , case
            when nullif(trim(p."Dự án con (nếu có)"), '') is null
                or upper(trim(p."Dự án con (nếu có)")) = '#N/A'
                or lower(normalize(trim(p."Dự án con (nếu có)"), nfc)) = 'không có'
                then 'Không có dự án con'
            else nullif(regexp_replace(normalize(trim(p."Dự án con (nếu có)"), nfc), '\s+', ' ', 'g'), '')
          end as sub_project_name
    from {{ source('staging', 'resource_performance') }} p
    where nullif(trim(p."Dự án chốt"), '') is not null
        and upper(trim(p."Dự án chốt")) <> '#N/A'
        and lower(normalize(trim(p."Dự án chốt"), nfc)) <> 'không xác định'
),

from_performance as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'pc.repository_name',
            'cast(csp.canonical_sub_project_id as text)'
        ]) }} as repository_id
        , cast(csp.canonical_sub_project_id as text) as sub_project_id
        , pc.repository_name
        , 3 as source_priority
    from performance_clean pc
    inner join canonical_projects cp
        on lower(normalize(pc.project_name, nfc))
            = lower(normalize(cp.project_name, nfc))
    inner join canonical_sub_projects csp
        on csp.canonical_project_id = cp.canonical_project_id
        and lower(normalize(pc.sub_project_name, nfc))
            = lower(normalize(csp.sub_project_name, nfc))
    where not exists (
        select 1
        from from_odoo fo
        where fo.sub_project_id = cast(csp.canonical_sub_project_id as text)
            and lower(normalize(fo.repository_name, nfc))
                = lower(normalize(pc.repository_name, nfc))
    )
),

combined_base as (
    select * from from_odoo
    union all
    select * from from_partners
    union all
    select * from from_performance
),

normalised_combined_base as (
    select
        repository_id
        , sub_project_id
        , nullif(regexp_replace(normalize(trim(repository_name), nfc), '\s+', ' ', 'g'), '') as repository_name
        , source_priority
    from combined_base
    where repository_id is not null
        and sub_project_id is not null
        and nullif(trim(repository_name), '') is not null
),

deduped_base as (
    select distinct on (
        sub_project_id,
        lower(normalize(repository_name, nfc))
    )
        repository_id
        , sub_project_id
        , repository_name
    from normalised_combined_base
    order by
        sub_project_id
        , lower(normalize(repository_name, nfc))
        , case when repository_id ~ '^[0-9]+$' then 0 else 1 end
        , case when repository_id ~ '^[0-9]+$' then repository_id::numeric end nulls last
        , source_priority
        , repository_id
),

-- Approved aliases are scoped to the parent project. This keeps a confirmed
-- spelling correction from becoming a global fuzzy-match rule.
distro_sub_project_name_aliases as (
    select *
    from (
        values
            ('classical', 'classcial bật cid', 'classical bật cid')
    ) as aliases(
        project_name_key,
        source_sub_project_name_key,
        target_sub_project_name_key
    )
),

-- A repository is valid only when both its Distro project and sub-project
-- resolve to the current canonical dimensions. Compound source values are not
-- split, because their hierarchy is ambiguous.
distro_repository_values as (
    select
        nullif(regexp_replace(normalize(trim(cast(distro."Dự án" as text)), nfc), '\s+', ' ', 'g'), '') as project_name
        , nullif(regexp_replace(normalize(trim(cast(distro."Dự án con" as text)), nfc), '\s+', ' ', 'g'), '') as sub_project_name
        , nullif(regexp_replace(normalize(trim(cast(distro."Kho" as text)), nfc), '\s+', ' ', 'g'), '') as repository_name
    from {{ source('staging', 'distro_infomation') }} distro
    where nullif(trim(cast(distro."Dự án" as text)), '') is not null
        and nullif(trim(cast(distro."Dự án con" as text)), '') is not null
        and nullif(trim(cast(distro."Kho" as text)), '') is not null
),

distro_repository_names as (
    select distinct on (sub_project_id, repository_name_key)
        sub_project_id
        , repository_name
        , repository_name_key
    from (
        select
            cast(csp.canonical_sub_project_id as text) as sub_project_id
            , distro.repository_name
            , lower(distro.repository_name) as repository_name_key
        from distro_repository_values distro
        inner join canonical_projects cp
            on lower(cp.project_name) = lower(distro.project_name)
        left join distro_sub_project_name_aliases aliases
            on lower(distro.project_name) = aliases.project_name_key
            and lower(distro.sub_project_name) = aliases.source_sub_project_name_key
        inner join canonical_sub_projects csp
            on csp.canonical_project_id = cp.canonical_project_id
            and lower(csp.sub_project_name) = coalesce(
                aliases.target_sub_project_name_key,
                lower(distro.sub_project_name)
            )
        where distro.project_name is not null
            and distro.sub_project_name is not null
            and distro.repository_name is not null
            and lower(distro.project_name) <> 'dự án'
            and lower(distro.sub_project_name) <> 'dự án con'
            and lower(distro.repository_name) <> 'kho'
    ) distro
    where sub_project_id is not null
    order by sub_project_id, repository_name_key, repository_name
),

from_distro_information as (
    select
        coalesce(
            base.repository_id
            , {{ dbt_utils.generate_surrogate_key([
                'distro.sub_project_id',
                'distro.repository_name_key'
            ]) }}
        ) as repository_id
        , distro.sub_project_id
        , coalesce(base.repository_name, distro.repository_name) as repository_name
    from distro_repository_names distro
    left join deduped_base base
        on base.sub_project_id = distro.sub_project_id
        and lower(regexp_replace(normalize(trim(base.repository_name), nfc), '\s+', ' ', 'g'))
            = distro.repository_name_key
),

-- Resource-information rows are accepted only when their project hierarchy
-- resolves to the canonical dimensions. Repository names are intentionally not
-- fuzzy-matched: reviewed variants remain separate repositories.
resource_information_add_clean as (
    select distinct
        nullif(regexp_replace(normalize(trim(cast(resource_add."Kho" as text)), nfc), '\s+', ' ', 'g'), '') as repository_name
        , nullif(regexp_replace(normalize(trim(cast(resource_add."Dự án" as text)), nfc), '\s+', ' ', 'g'), '') as project_name
        , case
            when nullif(trim(cast(resource_add."Dự án con" as text)), '') is null
                or lower(normalize(trim(cast(resource_add."Dự án con" as text)), nfc)) in ('không có', 'khong co')
                then 'Không có dự án con'
            else nullif(regexp_replace(normalize(trim(cast(resource_add."Dự án con" as text)), nfc), '\s+', ' ', 'g'), '')
          end as sub_project_name
    from {{ source('staging', 'resource_infomation_add') }} resource_add
    where nullif(trim(cast(resource_add."Kho" as text)), '') is not null
        and lower(normalize(trim(cast(resource_add."Kho" as text)), nfc)) not in ('không có', 'khong co')
        and nullif(trim(cast(resource_add."Dự án" as text)), '') is not null
),

resource_information_add_repository_names as (
    select distinct on (sub_project_id, repository_name_key)
        sub_project_id
        , repository_name
        , repository_name_key
    from (
        select
            cast(csp.canonical_sub_project_id as text) as sub_project_id
            , resource_add.repository_name
            , lower(normalize(resource_add.repository_name, nfc)) as repository_name_key
        from resource_information_add_clean resource_add
        inner join canonical_projects cp
            on lower(normalize(cp.project_name, nfc))
                = lower(normalize(resource_add.project_name, nfc))
        inner join canonical_sub_projects csp
            on csp.canonical_project_id = cp.canonical_project_id
            and lower(normalize(csp.sub_project_name, nfc))
                = lower(normalize(resource_add.sub_project_name, nfc))
        where resource_add.repository_name is not null
            and resource_add.project_name is not null
            and resource_add.sub_project_name is not null
    ) resource_add
    order by sub_project_id, repository_name_key, repository_name
),

combined_with_distro as (
    select repository_id, sub_project_id, repository_name from deduped_base
    union all
    select repository_id, sub_project_id, repository_name from from_distro_information
),

deduped_with_distro as (
    select distinct on (
        sub_project_id,
        lower(normalize(repository_name, nfc))
    )
        repository_id
        , sub_project_id
        , repository_name
    from combined_with_distro
    order by
        sub_project_id
        , lower(normalize(repository_name, nfc))
        , case when repository_id ~ '^[0-9]+$' then 0 else 1 end
        , case when repository_id ~ '^[0-9]+$' then repository_id::numeric end nulls last
        , repository_id
),

from_resource_information_add as (
    select
        coalesce(
            existing.repository_id
            , {{ dbt_utils.generate_surrogate_key([
                'resource_add.sub_project_id',
                'resource_add.repository_name_key'
            ]) }}
        ) as repository_id
        , resource_add.sub_project_id
        , coalesce(existing.repository_name, resource_add.repository_name) as repository_name
    from resource_information_add_repository_names resource_add
    left join deduped_with_distro existing
        on existing.sub_project_id = resource_add.sub_project_id
        and lower(regexp_replace(normalize(trim(existing.repository_name), nfc), '\s+', ' ', 'g'))
            = resource_add.repository_name_key
),

combined as (
    select
        repository_id
        , sub_project_id
        , repository_name
    from deduped_base

    union all

    select
        repository_id
        , sub_project_id
        , repository_name
    from from_distro_information

    union all

    select
        repository_id
        , sub_project_id
        , repository_name
    from from_resource_information_add
),

deduped as (
    select distinct on (
        sub_project_id,
        lower(normalize(repository_name, nfc))
    )
        repository_id
        , sub_project_id
        , repository_name
    from combined
    order by
        sub_project_id
        , lower(normalize(repository_name, nfc))
        , case when repository_id ~ '^[0-9]+$' then 0 else 1 end
        , case when repository_id ~ '^[0-9]+$' then repository_id::numeric end nulls last
        , repository_id
),

default_per_sub_project as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(csp.canonical_sub_project_id as text)',
            "'Không có kho'"
        ]) }} as repository_id
        , cast(csp.canonical_sub_project_id as text) as sub_project_id
        , 'Không có kho' as repository_name
    from canonical_sub_projects csp
    where not exists (
        select 1
        from deduped d
        where d.sub_project_id = cast(csp.canonical_sub_project_id as text)
            and lower(normalize(d.repository_name, nfc))
                = lower(normalize('Không có kho', nfc))
    )
),

with_default as (
    select * from deduped
    union all
    select * from default_per_sub_project
)

select distinct on (repository_id)
    {{ dbt_utils.generate_surrogate_key(['repository_id']) }} as dim_repository_sk
    , repository_id
    , sub_project_id
    , repository_name
    , case
        when lower(normalize(repository_name, nfc))
                = lower(normalize('Không có kho', nfc))
            or repository_name ~ '[-0-9]'
            then 'Sản xuất'
        else 'Thu mua'
      end as repository_type
from with_default
order by repository_id, sub_project_id, repository_name
