select accident_id, reg_code, cast(dt as date) as day,
    dead_n, injured_n, vehicles_n
from {{ source('raw', 'accidents') }}
