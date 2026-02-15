with customers as(
    select *
    from {{ ref('stg_olist_customers')}}
)


select
 customer_id,
    customer_unique_id,
    customer_city,
    customer_state
from customers