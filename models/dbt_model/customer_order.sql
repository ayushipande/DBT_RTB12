select
    a.customer_id,
    a.first_name,
    a.last_name,
    b.order_id,
    b.order_date,
    b.status as order_status
from {{ ref("stg_customers") }} a
inner join {{ ref("stg_orders") }} as b on a.customer_id = b.customer_id
