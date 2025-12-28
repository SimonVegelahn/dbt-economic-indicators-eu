{% snapshot snap_gdp_history %}

{{
    config(
        target_schema='snapshots',
        unique_key='gdp_key',
        strategy='check',
        check_cols=['gdp_million_eur'],
        invalidate_hard_deletes=True
    )
}}

/*
    Snapshot: GDP data with full history tracking.
    
    This snapshot uses the 'check' strategy to detect changes in GDP values.
    Useful for tracking data revisions, which are common in economic statistics.
    
    Eurostat frequently revises historical GDP figures as new data becomes
    available. This snapshot preserves the full revision history.
    
    Use cases:
    - Audit trail for data changes
    - Analysis of revision patterns
    - Point-in-time reporting
*/

select
    gdp_key,
    country_code,
    country_name,
    reference_year,
    gdp_million_eur,
    unit_code,
    source_dataset,
    _extracted_at
from {{ ref('stg_eurostat__gdp') }}

{% endsnapshot %}
