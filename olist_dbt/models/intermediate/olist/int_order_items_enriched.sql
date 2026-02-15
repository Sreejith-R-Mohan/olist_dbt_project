with order_items as (
    select *
    from {{ref('stg_olist_order_items')}}
),
orders as(
   select *
    from {{ref('stg_olist_orders')}}
),
products as(
    select *
    from {{ref('stg_olist_products')}}
),
joined as(
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,

        oi.price,
        oi.freight_value,

        (oi.price + oi.freight_value) as item_total_amount,

        o.ORDER_PURCHASE_TIMESTAMP,
        o.order_status,

        p.product_category_name

    from order_items oi
     left join orders o
        on oi.order_id = o.order_id
    left join products p
        on oi.product_id = p.product_id
)

select * from joined