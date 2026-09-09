-- Replacement model for dim_project.
-- Project names are stored as NFC, trimmed and with repeated whitespace collapsed.
-- RBO, Distro, and resource_infomation_add are first-class project sources.

{#
  In an existing HG DWH, reuse the previous dim_project relation to preserve
  stable project IDs for supplemental project names. A clean local rebuild has
  no previous relation, so expose an empty relation with the same shape instead
  of self-referencing a table that does not exist yet.
#}
{% set existing_dim_project_relation = adapter.get_relation(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier
) %}

with from_odoo as (
    select
        nullif(trim(cast(id as text)), '') as project_id
        , nullif(regexp_replace(normalize(trim(cast(name as text)), nfc), '\s+', ' ', 'g'), '') as project_name
        , nullif(trim(cast(state as text)), '') as status
    from {{ source('staging', 'x_project') }}
),

from_partners as (
    select
        {{ dbt_utils.generate_surrogate_key(['"Dự án"']) }} as project_id
        , nullif(regexp_replace(normalize(trim("Dự án"), nfc), '\s+', ' ', 'g'), '') as project_name
        , cast(null as text) as status
    from {{ source('staging', 'partners') }}
    where nullif(trim("Dự án"), '') is not null
),

rbo_project_names as (
    select distinct on (project_name_key)
        project_name
        , project_name_key
    from (
        select
            nullif(regexp_replace(normalize(trim(rbo."Thể loại"), nfc), '\s+', ' ', 'g'), '') as project_name
            , lower(regexp_replace(normalize(trim(rbo."Thể loại"), nfc), '\s+', ' ', 'g')) as project_name_key
        from {{ source('staging', 'resource_before_odoo') }} rbo
        where nullif(trim(rbo."Thể loại"), '') is not null
    ) rbo
    where project_name is not null
    order by project_name_key, project_name
),

from_resource_before_odoo as (
    select
        {{ dbt_utils.generate_surrogate_key(['project_name_key']) }} as project_id
        , project_name
        , cast(null as text) as status
    from rbo_project_names
),

performance_projects as (
    select distinct on (project_name_key)
        project_name
        , project_name_key
    from (
        select
            nullif(regexp_replace(normalize(trim(p."Dự án chốt"), nfc), '\s+', ' ', 'g'), '') as project_name
            , lower(regexp_replace(normalize(trim(p."Dự án chốt"), nfc), '\s+', ' ', 'g')) as project_name_key
        from {{ source('staging', 'resource_performance') }} p
        where nullif(trim(p."Dự án chốt"), '') is not null
            and upper(trim(p."Dự án chốt")) <> '#N/A'
            and lower(normalize(trim(p."Dự án chốt"), nfc)) <> 'không xác định'
    ) performance
    where project_name is not null
    order by project_name_key, project_name
),

from_performance as (
    select
        {{ dbt_utils.generate_surrogate_key(['project_name_key']) }} as project_id
        , project_name
        , cast(null as text) as status
    from performance_projects
),

existing_dim_project as (
    {% if existing_dim_project_relation is not none %}
    select distinct on (project_name_key)
        project_id
        , project_name
        , status
        , project_name_key
    from (
        select
            nullif(trim(cast(project_id as text)), '') as project_id
            , nullif(regexp_replace(normalize(trim(project_name), nfc), '\s+', ' ', 'g'), '') as project_name
            , status
            , lower(regexp_replace(normalize(trim(project_name), nfc), '\s+', ' ', 'g')) as project_name_key
        from {{ existing_dim_project_relation }}
    ) existing
    where project_id is not null
        and project_name is not null
    order by
        project_name_key
        , case when project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when project_id ~ '^[0-9]+$' then project_id::numeric end nulls last
        , project_id
    {% else %}
    select
        cast(null as text) as project_id
        , cast(null as text) as project_name
        , cast(null as text) as status
        , cast(null as text) as project_name_key
    where false
    {% endif %}
),

-- A slash in Distro's project column represents multiple projects.
distro_project_values as (
    select
        nullif(regexp_replace(normalize(trim(project_value), nfc), '\s+', ' ', 'g'), '') as project_name
    from {{ source('staging', 'distro_infomation') }} distro
    cross join lateral regexp_split_to_table(cast(distro."Dự án" as text), '\s*/\s*') as project_value
    where nullif(trim(cast(distro."Dự án" as text)), '') is not null
),

distro_project_names as (
    select distinct on (project_name_key)
        project_name
        , project_name_key
    from (
        select
            project_name
            , lower(project_name) as project_name_key
        from distro_project_values
        where project_name is not null
            and lower(project_name) <> 'dự án'
    ) distro
    order by project_name_key, project_name
),

-- Non-exact decisions stay explicit; do not use a broad fuzzy-match rule.
distro_project_name_aliases as (
    select *
    from (
        values
            ('indie pop', 'indie pop - 2')
    ) as aliases(source_project_name_key, target_project_name_key)
),

from_distro_information as (
    select
        coalesce(
            existing.project_id
            , {{ dbt_utils.generate_surrogate_key(['distro.project_name_key']) }}
        ) as project_id
        , coalesce(existing.project_name, distro.project_name) as project_name
        , existing.status as status
    from distro_project_names distro
    left join distro_project_name_aliases aliases
        on distro.project_name_key = aliases.source_project_name_key
    left join existing_dim_project existing
        on existing.project_name_key = coalesce(aliases.target_project_name_key, distro.project_name_key)
),

-- resource_infomation_add supplements project coverage. Whitespace, casing,
-- and Unicode composition are normalised; distinct project names are retained.
resource_information_add_projects as (
    select distinct on (project_name_key)
        project_name
        , project_name_key
    from (
        select
            nullif(regexp_replace(normalize(trim(cast(resource_add."Dự án" as text)), nfc), '\s+', ' ', 'g'), '') as project_name
            , lower(regexp_replace(normalize(trim(cast(resource_add."Dự án" as text)), nfc), '\s+', ' ', 'g')) as project_name_key
        from {{ source('staging', 'resource_infomation_add') }} resource_add
        where nullif(trim(cast(resource_add."Dự án" as text)), '') is not null
            and upper(trim(cast(resource_add."Dự án" as text))) <> '#N/A'
            and lower(normalize(trim(cast(resource_add."Dự án" as text)), nfc)) not in (
                'không xác định',
                'bỏ bài này',
                'dự án'
            )
    ) resource_add
    where project_name is not null
    order by project_name_key, project_name
),

from_resource_information_add as (
    select
        coalesce(
            existing.project_id
            , {{ dbt_utils.generate_surrogate_key(['resource_add.project_name_key']) }}
        ) as project_id
        , coalesce(existing.project_name, resource_add.project_name) as project_name
        , existing.status as status
    from resource_information_add_projects resource_add
    left join existing_dim_project existing
        on existing.project_name_key = resource_add.project_name_key
),

combined as (
    select * from from_odoo
    union all
    select * from from_partners
    union all
    select * from from_resource_before_odoo
    union all
    select * from from_performance
    union all
    select * from from_distro_information
    union all
    select * from from_resource_information_add
),

normalised_combined as (
    select
        project_id
        , nullif(regexp_replace(normalize(trim(project_name), nfc), '\s+', ' ', 'g'), '') as project_name
        , status
        , lower(regexp_replace(normalize(trim(project_name), nfc), '\s+', ' ', 'g')) as project_name_key
    from combined
    where project_id is not null
        and nullif(trim(project_name), '') is not null
),

canonical_by_name as (
    select distinct on (project_name_key)
        project_id
        , project_name
        , status
    from normalised_combined
    order by
        project_name_key
        , case when project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when project_id ~ '^[0-9]+$' then project_id::numeric end nulls last
        , project_id
        , project_name
        , status nulls last
),

canonical_by_id as (
    select distinct on (project_id)
        project_id
        , project_name
        , status
    from canonical_by_name
    order by project_id, project_name, status nulls last
)

select
    {{ dbt_utils.generate_surrogate_key(['project_id']) }} as dim_project_sk
    , project_id
    , project_name
    , status
from canonical_by_id
