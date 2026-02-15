with source as (
    Select
    *
    from 
    {{ source('raw_olist','orders') }}
),
cleaned as(
SELECT 
ORDER_ID, 
CUSTOMER_ID, 
lower(ORDER_STATUS) as order_status,
cast(ORDER_PURCHASE_TIMESTAMP as timestamp) as ORDER_PURCHASE_TIMESTAMP, 
cast(ORDER_APPROVED_AT as timestamp) as ORDER_APPROVED_AT, 
cast(ORDER_DELIVERED_CARRIER_DATE as timestamp) as ORDER_DELIVERED_CARRIER_DATE, 
cast(ORDER_DELIVERED_CUSTOMER_DATE as timestamp) as ORDER_DELIVERED_CUSTOMER_DATE, 
cast(ORDER_ESTIMATED_DELIVERY_DATE as timestamp) as ORDER_ESTIMATED_DELIVERY_DATE
FROM source
),
dedepulicated as (
    select *
    from cleaned qualify row_number() over (
        partition by order_id
        order by ORDER_PURCHASE_TIMESTAMP desc
    ) = 1
)


select * from dedepulicated