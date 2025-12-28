{{
    config(
        materialized='incremental',
        unique_key='indicator_key',
        on_schema_change='append_new_columns',
        tags=['marts', 'facts', 'incremental'],
        contract={'enforced': true}
    )
}}

/*
    Fact table: Economic indicators at monthly grain.
    
    This is the primary fact table for economic analysis. It uses
    incremental materialization to efficiently process new data.
    
    Grain: One row per country per month
    
    Usage:
    - Dashboard visualizations
    - Ad-hoc economic analysis
    - Cross-country benchmarking
*/

with monthly_indicators as (
    select * from {{ ref('int_country_monthly_indicators') }}
    
    {% if is_incremental() %}
    -- Only process new data in incremental runs
    where reference_date > (select max(reference_date) from {{ this }})
    {% endif %}
),

country_dim as (
    select * from {{ ref('dim_country') }}
),

final as (
    select
        -- Primary key
        mi.monthly_metrics_key as indicator_key,
        
        -- Foreign keys
        mi.country_code,
        cd.country_key,
        
        -- Time dimensions
        mi.reference_year,
        mi.reference_month,
        mi.reference_date,
        
        -- Core indicators
        mi.unemployment_rate_pct,
        mi.inflation_rate_mom_pct,
        
        -- Trend metrics
        mi.unemployment_rate_prev_month,
        mi.unemployment_rate_prev_year,
        mi.unemployment_rate_12m_avg,
        mi.inflation_rate_12m_avg,
        
        -- Change calculations
        mi.unemployment_rate_pct - mi.unemployment_rate_prev_month as unemployment_mom_change,
        mi.unemployment_rate_pct - mi.unemployment_rate_prev_year as unemployment_yoy_change,
        
        -- Annual context
        mi.annual_gdp_million_eur,
        mi.annual_population_count,
        mi.annual_gdp_per_capita_eur,
        
        -- Metadata
        current_timestamp as _loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id
        
    from monthly_indicators mi
    left join country_dim cd on mi.country_code = cd.country_code
)

select * from final
