with
    first_order as (
        select customer_id, min(order_date) as first_order
        from {{ ref("stg_orders") }}
        where status = 'completed'
        group by customer_id
    ),
    recent_order as (
        select customer_id, max(order_date) as recent_order
        from {{ ref("stg_orders") }}
        where status = 'completed'
        group by customer_id
    ),
    order_count as (
        select customer_id, count(order_id) as total_number_of_orders
        from {{ ref("stg_orders") }}
        where status = 'completed'
        group by customer_id
    ),
    total_payments as (
        select customer_id, sum(amount) as total_payments
        from {{ ref("stg_orders") }} a
        inner join {{ ref("stg_payments") }} b on a.order_id = b.order_id
        where status = 'completed'
        group by customer_id
    )

select
    a.customer_id,
    a.first_name,
    a.last_name,
    first_order,
    recent_order,
    nvl(total_number_of_orders, 0) total_number_of_orders,
    nvl(total_payments, 0) total_payments

from {{ ref("stg_customers") }} a
left join first_order on a.customer_id = first_order.customer_id
left join recent_order on a.customer_id = recent_order.customer_id
left join order_count on a.customer_id = order_count.customer_id
left join total_payments on a.customer_id = total_payments.customer_id
