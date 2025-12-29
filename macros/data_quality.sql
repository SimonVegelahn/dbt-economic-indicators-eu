{% macro test_value_in_range(model, column_name, min_value, max_value) %}
/*
    Generic test to check if values fall within expected range.
    
    Usage in schema.yml:
        tests:
          - test_value_in_range:
              min_value: 0
              max_value: 100
*/
select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}
{% endmacro %}


{% macro test_no_future_dates(model, column_name) %}
/*
    Test that a date column doesn't contain future dates.
    
    Useful for ensuring data extraction hasn't included invalid data.
*/
select *
from {{ model }}
where {{ column_name }} > current_date
{% endmacro %}


{% macro test_completeness_threshold(model, column_name, threshold=0.95) %}
/*
    Test that a column has at least threshold% non-null values.
    
    Args:
        threshold: Minimum percentage of non-null values (default: 0.95 = 95%)
*/
with stats as (
    select
        count(*) as total_rows,
        count({{ column_name }}) as non_null_rows
    from {{ model }}
)
select *
from stats
where (non_null_rows::float / total_rows) < {{ threshold }}
{% endmacro %}


{% macro log_row_count(relation) %}
/*
    Log the row count of a relation during model execution.
    Useful for monitoring data volumes.
*/
{% set row_count_query %}
    select count(*) as cnt from {{ relation }}
{% endset %}

{% if execute %}
    {% set results = run_query(row_count_query) %}
    {% set row_count = results.columns[0].values()[0] %}
    {{ log("Row count for " ~ relation ~ ": " ~ row_count, info=True) }}
{% endif %}
{% endmacro %}


{% macro generate_schema_name(custom_schema_name, node) %}
{# Custom schema naming: dev = dev_schema, prod = schema #}
{% if target.name == 'prod' %}
{{ custom_schema_name | trim }}
{% else %}
{{ target.name }}_{{ custom_schema_name | trim }}
{% endif %}
{% endmacro %}
