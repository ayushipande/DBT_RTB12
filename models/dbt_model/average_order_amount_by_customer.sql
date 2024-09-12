with
    order_payments as (
        select o.customer_id, o.order_id, sum(p.amount) as total_order_amount
        from {{ ref("stg_orders") }} o
        left join {{ ref("stg_payments") }} p on o.order_id = p.order_id
        where o.status = 'completed'
        group by o.customer_id, o.order_id
    ),
    customer_order_totals as (
        select
            customer_id,
            count(order_id) as total_orders,
            sum(total_order_amount) as total_payments
        from order_payments
        group by customer_id
    )
select
    c.customer_id,
    c.first_name,
    c.last_name,
    coalesce(customer_order_totals.total_orders, 0) as total_orders,
    coalesce(customer_order_totals.total_payments, 0) as total_payments,
    case
        when customer_order_totals.total_orders > 0
        then customer_order_totals.total_payments / customer_order_totals.total_orders
        else 0
    end as average_order_amount
from {{ ref("stg_customers") }} c
left join customer_order_totals on c.customer_id = customer_order_totals.customer_id
