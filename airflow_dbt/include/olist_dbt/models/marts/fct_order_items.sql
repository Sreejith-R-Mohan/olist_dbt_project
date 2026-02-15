select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    item_total_amount,
    ORDER_PURCHASE_TIMESTAMP,
    order_status
from {{ ref('int_order_items_enriched') }}
