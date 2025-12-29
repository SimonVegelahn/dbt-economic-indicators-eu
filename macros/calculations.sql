{% macro generate_surrogate_key(field_list) %}
/*
    Generate a surrogate key from a list of fields using MD5 hash.
    Local replacement for dbt_utils.generate_surrogate_key.

    Args:
        field_list: List of column names to concatenate and hash

    Returns:
        MD5 hash of concatenated fields
*/
md5(concat_ws('||', {% for field in field_list %}coalesce(cast({{ field }} as varchar), ''){% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}


{% macro calculate_yoy_change(value_column, partition_column, order_column) %}
/*
    Calculate year-over-year change for a given metric.
    
    Args:
        value_column: The column containing the metric value
        partition_column: Column to partition by (e.g., country_code)
        order_column: Column to order by (e.g., reference_year)
    
    Returns:
        Year-over-year change as: (current - previous) / previous * 100
*/
case 
    when lag({{ value_column }}) over (
        partition by {{ partition_column }} 
        order by {{ order_column }}
    ) is not null 
    and lag({{ value_column }}) over (
        partition by {{ partition_column }} 
        order by {{ order_column }}
    ) != 0
    then (
        ({{ value_column }} - lag({{ value_column }}) over (
            partition by {{ partition_column }} 
            order by {{ order_column }}
        )) / lag({{ value_column }}) over (
            partition by {{ partition_column }} 
            order by {{ order_column }}
        ) * 100
    )
    else null
end
{% endmacro %}


{% macro calculate_rolling_average(value_column, partition_column, order_column, periods=12) %}
/*
    Calculate rolling average for a given metric.
    
    Args:
        value_column: The column containing the metric value
        partition_column: Column to partition by (e.g., country_code)
        order_column: Column to order by (e.g., reference_date)
        periods: Number of periods for the rolling window (default: 12)
    
    Returns:
        Rolling average over the specified window
*/
avg({{ value_column }}) over (
    partition by {{ partition_column }} 
    order by {{ order_column }} 
    rows between {{ periods - 1 }} preceding and current row
)
{% endmacro %}


{% macro rank_within_group(value_column, partition_column, ascending=true) %}
/*
    Rank values within a group.
    
    Args:
        value_column: The column to rank by
        partition_column: Column to partition by (e.g., reference_year)
        ascending: If true, rank from lowest to highest (default: true)
    
    Returns:
        Rank within the partition
*/
rank() over (
    partition by {{ partition_column }} 
    order by {{ value_column }} {% if ascending %}asc{% else %}desc{% endif %}
)
{% endmacro %}


{% macro safe_divide(numerator, denominator, default=0) %}
/*
    Safely divide two numbers, returning default on division by zero.
    
    Args:
        numerator: The dividend
        denominator: The divisor
        default: Value to return if denominator is 0 or null (default: 0)
    
    Returns:
        numerator / denominator or default
*/
case 
    when {{ denominator }} is null or {{ denominator }} = 0 
    then {{ default }}
    else {{ numerator }} / {{ denominator }}
end
{% endmacro %}
