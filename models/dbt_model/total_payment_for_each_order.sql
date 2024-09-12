select a.order_id, sum(coalesce(p.amount, 0)) as total_amount
from {{ ref("stg_orders") }} as a
left join {{ ref("stg_payments") }} as p on a.order_id = p.order_id
group by a.order_id
