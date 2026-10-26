select
    menu_id,
    try_to_number(r_id) as restaurant_id,
    f_id,
    cuisine,
    try_to_decimal(price,10,2) as price
from {{ source('raw', 'menu') }} where try_to_number(r_id) is not null and try_to_decimal(price,10,2) > 0
-- a menu_id lists several food items, so the key is (menu_id, f_id)
qualify row_number() over (partition by menu_id, f_id order by _loaded_at desc nulls last, _source_file desc) = 1
