select * from {{ ref('fact_daily') }}
where dead < 0 or injured < 0 or victims != dead + injured
