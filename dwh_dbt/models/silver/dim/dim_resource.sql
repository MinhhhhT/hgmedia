-- silver.dim_resource  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as dim_resource_sk
    , nullif(trim(cast(id as text)),'') as resource_id
    , nullif(trim(cast(subgenre_id as text)),'') as repository_id
    , nullif(trim(cast(song_name as text)),'') as resource_name
    , nullif(trim(cast(name as text)),'') as song_code
    , cast(nullif(trim(cast(review_score as text)),'') as numeric(18,2)) as acceptance_score
    , cast(nullif(trim(cast(review_price as text)),'') as numeric(18,2)) as acceptance_cost
    , cast(nullif(trim(cast(review_date as text)),'') as timestamp) as acceptance_date
    , nullif(trim(cast(detail_line_id as text)),'') as production_plan_detail_id
    , case
        when state = 'approved' and produce_state = 6 then 'Không nghiệm thu'
        when state = 'approved' and produce_state is distinct from 6 then 'Đã nghiệm thu'
        when state = 'draft' then 'Đang sản xuất'
        when state = 'rejected' then 'Không nghiệm thu'
        else null
      end as status
    , nullif(trim(cast(sale_order_id as text)),'') as so_id
    , nullif(trim(cast(purchase_order_line_id as text)),'') as po_detail_id
from {{ source('staging', 'x_music_song') }}
