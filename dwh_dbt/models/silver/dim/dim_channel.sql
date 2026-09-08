with channel_user_map as (

    select distinct on (cu."ChannelId")
        cu."ChannelId" as channel_id
        , u."FullName" as employee_name
    from {{ source('staging', 'channel_user') }} cu
    left join {{ source('staging', 'user') }} u
        on cu."UserId" = u."Id"
    where cu."IsDeleted" = false
    order by
        cu."ChannelId"
        , cu."CreatedTimeUtc" desc

),

channel_project_map as (

    -- Chỉ lấy mapping còn hiệu lực; nếu một kênh có nhiều mapping active,
    -- chọn mapping được tạo mới nhất.
    select distinct on (cp."ChannelId")
        cp."ChannelId" as channel_id
        , cp."ProjectId" as assigned_project_id
        , p."ParentId" as parent_project_id
    from {{ source('staging', 'channel_project') }} cp
    left join {{ source('staging', 'project') }} p
        on cp."ProjectId" = p."Id"
    where coalesce(cp."IsDeleted", false) = false
        and nullif(trim(cast(cp."ProjectId" as text)), '') is not null
    order by
        cp."ChannelId"
        , cp."CreatedTimeUtc" desc nulls last
        , cp."Id" desc

),

channel_network_map as (

    select
        cd."ChannelId" as channel_id
        , n."Id" as network_id
    from {{ source('staging', 'channel_deal') }} cd
    left join {{ source('staging', 'cms') }} cms
        on cd."CmsId" = cms."Id"
    left join {{ source('staging', 'network') }} n
        on cms."NetworkId" = n."Id"
    where cd."IsDeleted" = false
        and cd."IsOutNet" = false

),

channel_company_map as (
    select distinct on (cd."ChannelId")
        cd."ChannelId" as channel_id
        , coalesce(
            case
                when dl."Name" like '%Công ty%' then d."Id"
            end
            , root_d."Id"
        ) as company_id
    from {{ source('staging', 'channeldepartment') }} cd
    inner join {{ source('staging', 'department') }} d
        on cd."DepartmentId" = d."Id"
    left join {{ source('staging', 'departmentlevel') }} dl
        on d."DepartmentLevelId" = dl."Id"
    left join {{ source('staging', 'department') }} root_d
        on root_d."Id" = nullif(
            split_part(
                trim(both '/' from coalesce(d."Path", '')),
                '/',
                1
            ),
            ''
        )
    order by
        cd."ChannelId"
        -- Ưu tiên mapping trực tiếp vào phòng ban cấp Công ty
        , case when dl."Name" like '%Công ty%' then 0 else 1 end
        , cd."CreatedTimeUtc" desc nulls last
        , cd."Id" desc
)

select distinct on (c."YoutubeChannelId")
    {{ dbt_utils.generate_surrogate_key(['c."YoutubeChannelId"']) }} as dim_channel_sk
    , nullif(trim(cast(c."YoutubeChannelId" as text)), '') as channel_id
    , nullif(trim(cast(c."Title" as text)), '') as channel_name
    , nullif(trim(cast(ccm.company_id as text)), '') as company_id
    , nullif(trim(cast(cum.employee_name as text)), '') as employee_name

    -- Nếu assigned_project_id là dự án con: lấy ParentId làm dự án cha.
    -- Nếu assigned_project_id là dự án cha: dùng chính ID đó.
    , nullif(
        trim(
            cast(
                coalesce(
                    cpm.parent_project_id
                    , cpm.assigned_project_id
                ) as text
            )
        ),
        ''
    ) as project_id

    -- Chỉ có dự án con khi project được gán có ParentId.
    , case
        when nullif(trim(cast(cpm.parent_project_id as text)), '') is not null
            then nullif(trim(cast(cpm.assigned_project_id as text)), '')
        else null
      end as sub_project_id

    , case
        when nullif(trim(cast(c."YoutubeChannelId" as text)), '') is not null
            then 'https://www.youtube.com/channel/'
                || trim(cast(c."YoutubeChannelId" as text))
        else null
      end as link
    , nullif(trim(cast(cnm.network_id as text)), '') as network_id
from {{ source('staging', 'channel') }} c
left join channel_project_map cpm
    on c."Id" = cpm.channel_id
left join channel_user_map cum
    on c."Id" = cum.channel_id
left join channel_network_map cnm
    on c."Id" = cnm.channel_id
left join channel_company_map ccm
    on c."Id" = ccm.channel_id
order by c."YoutubeChannelId"