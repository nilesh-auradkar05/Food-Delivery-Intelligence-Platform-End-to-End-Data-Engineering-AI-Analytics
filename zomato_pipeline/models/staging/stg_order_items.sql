select
    order_item_id, order_id, r_id as restaurant_id, f_id, price::decimal(10, 2) as
    price, quantity::number as quantity, line_amount::decimal(10, 2) as line_amount,
    coalesce(_loaded_at, '1970-01-01'::timestamp_ltz) as _loaded_at
from {{ source('raw', 'order_items') }}
qualify row_number() over (partition by order_item_id order by _loaded_at desc nulls last, _source_file desc) = 1
