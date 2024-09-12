select order_id
from (select distinct order_id, payment_method from {{ ref("stg_payments") }}) as a
group by order_id
having count(order_id) > 1
