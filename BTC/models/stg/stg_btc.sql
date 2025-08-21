{{ config(materialized='incremental', incremental_strategy='merge', unique_key='HASH_KEY') }}

select
*
from {{ source('btc', 'btc')}}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
where BLOCK_TIMESTAMP >= (select max(BLOCK_TIMESTAMP) from {{ this }} )
{% endif %}