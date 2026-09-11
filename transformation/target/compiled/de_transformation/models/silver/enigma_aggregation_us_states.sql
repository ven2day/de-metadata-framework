

SELECT
    A.state_fips AS s_fips,
    date_diff('day', cast(A.date as timestamp), lead(cast(B.date as timestamp)) over (partition by B.state_fips order by B.snapshot_date)) AS d_diff
FROM "minio"."de_bronze"."enigma_aggregation_us_states" A
    INNER JOIN "minio"."de_bronze"."enigma_aggregation_us_states" B
    ON B.state_fips = A.state_fips
WHERE A.snapshot_date = 
    DATE '2026-09-10'
  
  AND B.snapshot_date = 
    DATE '2026-09-10'
  