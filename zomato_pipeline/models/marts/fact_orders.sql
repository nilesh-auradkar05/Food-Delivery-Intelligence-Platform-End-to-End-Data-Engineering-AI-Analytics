{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}
select
    order_id, order_timestamp, order_date, customer_id, restaurant_id, city, cuisine,
    payment_method, order_status, is_delivered, items_count, sales_qty, subtotal, discount,
    delivery_fee, gst, sales_amount, customer_rating, delivery_time_min, _loaded_at
from {{ ref('stg_orders') }}
{% if is_incremental() and '_LOADED_AT' in (adapter.get_columns_in_relation(this) | map(attribute='name') | map('upper') | list) %}
    -- watermark skipped on the first run after _loaded_at was added: the target lacks the column, so that run re-merges everything
    -- load-time watermark: also picks up late-arriving orders and status corrections with old timestamps
    where _loaded_at > (select coalesce(max(t._loaded_at), '1900-01-01'::timestamp_ltz) from {{ this }} t)
{% endif %}
