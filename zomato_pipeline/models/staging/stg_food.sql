select
    f_id, item as food_name,
    initcap(veg_or_non_veg) as veg_or_non_veg,
from {{ source('raw', 'food') }} where f_id is not null
qualify row_number() over (partition by f_id order by _loaded_at desc nulls last, _source_file desc) = 1
