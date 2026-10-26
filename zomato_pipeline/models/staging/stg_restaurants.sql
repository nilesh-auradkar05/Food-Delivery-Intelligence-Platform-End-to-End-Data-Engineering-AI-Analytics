-- data formating rules:
-- 1. -- -> null
-- 2. 50+ ratings -> 50
-- 3. rs 200 -> 200
-- 4. city after last comma

select
    id::number as restaurant_id,
    name as restaurant_name,
    trim(coalesce(regexp_substr(city, '[^,]+$'), city)) as city,
    try_to_decimal(nullif(rating, '--'), 3, 1) as rating,
    try_to_number(regexp_substr(rating_count, '[0-9]+')) as rating_count,
    try_to_number(regexp_substr(cost, '[0-9]+')) as cost_for_two,
    cuisine,
    lic_no as license_number
from {{ source('raw', 'restaurants') }} where try_to_number(id) is not null
-- each snapshot file re-sends every restaurant: keep the latest version
qualify row_number() over (partition by restaurant_id order by _loaded_at desc nulls last, _source_file desc) = 1
