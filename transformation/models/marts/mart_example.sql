{{ config(materialized='table') }}

select *
from {{ ref('stg_bronze_example') }}
