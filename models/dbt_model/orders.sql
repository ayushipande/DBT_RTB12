with
    payment_details as (
        select
            order_id,
            sum(amount) as total_amount,
            sum(
                case when payment_method = 'credit_card' then amount else 0 end
            ) as credit_card_amount,
            sum(
                case when payment_method = 'bank_transfer' then amount else 0 end
            ) as bank_transfer_amount,
            sum(
                case when payment_method = 'coupon' then amount else 0 end
            ) as coupon_amount,
            sum(
                case when payment_method = 'gift_card' then amount else 0 end
            ) as gift_card_amount
        from {{ ref("stg_payments") }}
        group by order_id
    )

select
    a.order_id,
    a.customer_id,
    a.order_date,
    a.status as status,
    b.total_amount,
    b.credit_card_amount,
    b.bank_transfer_amount,
    b.coupon_amount,
    b.gift_card_amount
from {{ ref("stg_orders") }} as a
left join payment_details as b on a.order_id = b.order_id
