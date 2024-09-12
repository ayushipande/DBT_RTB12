with
    customer as (
        select customer_id, rank() over (order by total_amount desc) as row_number
        from
            (
                select a.customer_id, sum(p.amount) as total_amount
                from {{ ref("stg_customers") }} a
                inner join {{ ref("stg_orders") }} as b on a.customer_id = b.customer_id
                inner join {{ ref("stg_payments") }} as p on b.order_id = p.order_id
                group by a.customer_id
            ) as a
    )

select customer_id,
from customer
where row_number = 1
