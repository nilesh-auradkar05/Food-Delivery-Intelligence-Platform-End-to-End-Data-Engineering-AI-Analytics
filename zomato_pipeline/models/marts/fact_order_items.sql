{{ config(materialized='incremental', unique_key='order_item_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}
select oi.order_item_id, oi.order_id, oi.restaurant_id, oi.f_id, o.order_timestamp as order_ts,
      o.order_date, o.city, oi.price, oi.quantity, oi.line_amount,
      -- greatest(): an item whose order lands in a later file gets picked up when the order arrives
      greatest(oi._loaded_at, o._loaded_at) as _loaded_at
from {{ ref('stg_order_items') }} oi
inner join {{ ref('stg_orders') }} o using (order_id)
{% if is_incremental() and '_LOADED_AT' in (adapter.get_columns_in_relation(this) | map(attribute='name') | map('upper') | list) %}
    -- watermark skipped on the first run after _loaded_at was added: the target lacks the column, so that run re-merges everything
    where greatest(oi._loaded_at, o._loaded_at) > (select coalesce(max(t._loaded_at), '1900-01-01'::timestamp_ltz) from {{ this }} t)
{% endif %}