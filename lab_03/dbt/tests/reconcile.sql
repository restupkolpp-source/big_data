select 1 as mismatch
where (select sum(accidents) from {{ ref('fact_daily') }})
   != (select count(*) from {{ ref('stg_accidents') }})
