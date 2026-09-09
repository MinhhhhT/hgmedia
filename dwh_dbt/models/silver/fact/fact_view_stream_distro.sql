with sd as (
    select isrc, "dailyStream", "reportingDate", "idArtist"
    from {{ source('staging','stream_distro') }}
    where nullif(trim(isrc),'') is not null
),
plat as (
    select distinct isrc, retailer
    from {{ source('staging','sale') }}
    where nullif(trim(isrc),'') is not null
)
select
    {{ dbt_utils.generate_surrogate_key(['sd.isrc','sd."reportingDate"','sd."idArtist"']) }} as fact_view_stream_distro_sk
    , replace(cast("dailyStream" as text),',','')::numeric as stream_count
    , cast(nullif(trim(sd."reportingDate"),'') as date) as recorded_date
    , nullif(trim(p.retailer),'') as platform
    , nullif(trim(sd.isrc),'') as isrc
    , nullif(trim(sd."idArtist"),'') as artist
from sd
left join plat p on trim(sd.isrc) = trim(p.isrc)
