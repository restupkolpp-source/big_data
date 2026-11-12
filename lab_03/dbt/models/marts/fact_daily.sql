{% if var('break_model', false) %}
select missing_column from {{ ref('stg_accidents') }}
{% else %}
select a.day, a.reg_code, r.reg_name, count(*) as accidents,
       sum(a.dead_n) as dead, sum(a.injured_n) as injured,
       sum({{ victims('a.dead_n', 'a.injured_n') }}) as victims
from {{ ref('stg_accidents') }} a
join {{ ref('dim_regions') }} r using (reg_code)
group by a.day, a.reg_code, r.reg_name
{% endif %}
