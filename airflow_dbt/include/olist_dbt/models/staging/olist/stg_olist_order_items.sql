with source as (
    select *
    from
    {{source('raw_olist','order_items')}}
),
cleaned as (
    select 
    order_id,
    CAST(order_item_id AS INT) as order_item_id,
    product_id,
    seller_id,

    cast(SHIPPING_LIMIT_DATE as timestamp) as SHIPPING_LIMIT_DATE,
    cast(price as NUMERIC(10,2)) as price,
    cast(freight_value as NUMERIC(10,2)) as freight_value
    
    from source
),
depulicated as (
    select * 
    from cleaned
    qualify row_number() over(
        partition by order_id,order_item_id
        order by shipping_limit_date desc
    )=1
)

Select * from depulicated