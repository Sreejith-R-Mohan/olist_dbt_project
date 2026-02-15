with source as(
    Select
    *
    from
    {{ source('raw_olist', 'customers') }} 
),
cleaned as(
    select
    customer_id,
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ZIP_CODE_PREFIX,
    trim(CUSTOMER_CITY) as CUSTOMER_CITY,
    trim(CUSTOMER_STATE) as CUSTOMER_STATE
    from
    source
)


select * 
from cleaned