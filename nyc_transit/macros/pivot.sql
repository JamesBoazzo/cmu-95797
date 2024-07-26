{% macro pivot(column, values, agg='sum') %}
select
    {% for value in values %}
        {{ agg }}(case when {{ column }} = '{{ value }}' then num_trips else 0 end) as "{{ value }}"{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ this }}
{% endmacro %}



