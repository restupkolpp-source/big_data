{% macro victims(dead, injured) %}
    coalesce({{ dead }}, 0) + coalesce({{ injured }}, 0)
{% endmacro %}
