-- macros/export_pivoted_results.sql

{% macro export_pivoted_results(output_path) %}
    copy (
        select *
        from {{ ref('pivot_trips_by_borough') }}
    ) to {{ output_path }} (header, delimiter ',');
{% endmacro %}
