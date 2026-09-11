{{ config(materialized='view') }}

select *
from {{ source('de_bronze', 'bronze_table') }}
