{{
    config(
        materialized ='incremental',
        unique_key='order_id'
    )
}}

with orders as(
    select * from
    {{ ref('int_orders_enriched')}}
),

order_items as(
    select * from
    {{ref('int_order_items_enriched')}}
),
aggregated as(
        select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.ORDER_PURCHASE_TIMESTAMP,

        sum(oi.item_total_amount) as order_total_amount,
        count(oi.order_item_id) as total_items

    from orders o
    left join order_items oi
        on o.order_id = oi.order_id
    group by
        o.order_id,
        o.customer_id,
        o.order_status,
        o.ORDER_PURCHASE_TIMESTAMP 
)

select * from aggregated


{% if is_incremental() %}

where ORDER_PURCHASE_TIMESTAMP >(
    select max(ORDER_PURCHASE_TIMESTAMP)
    from {{this}}
)

{% endif %}
