with orders as (

    select *
    from {{ ref('stg_olist_orders') }}

),

customers as (

    select *
    from {{ ref('stg_olist_customers') }}

),

joined as(
    select 
        o.order_id,
        o.customer_id,
        o.order_status,
        o.ORDER_PURCHASE_TIMESTAMP,
        o.ORDER_APPROVED_AT,
        o.ORDER_DELIVERED_CUSTOMER_DATE,
        o.ORDER_ESTIMATED_DELIVERY_DATE,

        c.customer_unique_id,
        c.customer_city,
        c.customer_state
    from
    {{ref('stg_olist_orders')}} o left join {{ref('stg_olist_customers')}} c
    on o.customer_id = c.customer_id
)
select * from joined
