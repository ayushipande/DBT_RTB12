with
    avg_order_amount_cte as (
        select customer_id, average_order_amount
        from {{ ref("average_order_amount_by_customer") }}
    ),
    updated_cust_rpt as (
        select
            cust.customer_id,
            cust.first_name,
            cust.last_name,
            coalesce(
                avg_order_amount_cte.average_order_amount, cust.avg_order_amount
            ) as avg_order_amount
        from {{ ref("cust_rpt") }} as cust
        left join
            avg_order_amount_cte on cust.customer_id = avg_order_amount_cte.customer_id
    )
select *
from updated_cust_rpt
