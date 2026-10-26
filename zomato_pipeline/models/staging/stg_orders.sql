select
    order_id, order_timestamp, order_date, user_id as
    customer_id, r_id as restaurant_id,
    trim(coalesce(regexp_substr(restaurant_city, '[^,]+$'), restaurant_city)) as city,
    cuisine, items_count, sales_qty, subtotal, discount, delivery_fee, gst, sales_amount,
    currency, payment_method, order_status, (order_status='Delivered') as is_delivered,
    customer_rating, delivery_time_min,
    -- rows loaded before lineage columns existed count as the oldest load
    coalesce(_loaded_at, '1970-01-01'::timestamp_ltz) as _loaded_at
from {{ source('raw', 'orders') }}
-- same order in several files (reload or correction): latest load wins
qualify row_number() over (partition by order_id order by _loaded_at desc nulls last, _source_file desc) = 1
