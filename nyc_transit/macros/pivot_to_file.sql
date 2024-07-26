-- macros/pivot_to_file.sql

{% macro pivot_to_file(source_model, column_name, value_name, output_path) %}
    copy (
        select *
        from {{ dbt_utils.pivot(column_name, value_name, source_model) }}
    ) to {{ output_path }} (header, delimiter ',');
{% endmacro %}
