{{
    config(
        materialized='view',
        tags=['intermediate', 'monthly']
    )
}}

/*
    Intermediate model: Monthly economic indicators by country.
    
    Combines monthly unemployment and inflation data with
    annualized GDP/population context for time series analysis.
    
    Use cases:
    - Monthly trend analysis
    - Seasonal pattern detection
    - Cross-country comparison at monthly grain
*/

with unemployment as (
    select
        country_code,
        country_name,
        reference_year,
        reference_month,
        reference_date,
        unemployment_rate_pct
    from {{ ref('stg_eurostat__unemployment') }}
),

inflation as (
    select
        country_code,
        reference_year,
        reference_month,
        reference_date,
        inflation_rate_mom_pct
    from {{ ref('stg_eurostat__inflation') }}
),

-- Get annual context for each month
annual_context as (
    select
        country_code,
        reference_year,
        gdp_million_eur,
        population_count,
        gdp_per_capita_eur
    from {{ ref('int_country_annual_metrics') }}
),

monthly_spine as (
    -- Create complete monthly spine from unemployment data
    select distinct
        country_code,
        country_name,
        reference_year,
        reference_month,
        reference_date
    from unemployment
),

joined as (
    select
        -- Keys
        {{ generate_surrogate_key(['ms.country_code', 'ms.reference_date']) }} as monthly_metrics_key,
        
        -- Dimensions
        ms.country_code,
        ms.country_name,
        ms.reference_year,
        ms.reference_month,
        ms.reference_date,
        
        -- Monthly indicators
        u.unemployment_rate_pct,
        i.inflation_rate_mom_pct,
        
        -- Annual context (for normalization/comparison)
        ac.gdp_million_eur as annual_gdp_million_eur,
        ac.population_count as annual_population_count,
        ac.gdp_per_capita_eur as annual_gdp_per_capita_eur,
        
        -- Lag calculations for trend analysis
        lag(u.unemployment_rate_pct) over (
            partition by ms.country_code 
            order by ms.reference_date
        ) as unemployment_rate_prev_month,
        
        lag(i.inflation_rate_mom_pct) over (
            partition by ms.country_code 
            order by ms.reference_date
        ) as inflation_rate_prev_month,
        
        -- Year-over-year comparison
        lag(u.unemployment_rate_pct, 12) over (
            partition by ms.country_code 
            order by ms.reference_date
        ) as unemployment_rate_prev_year,
        
        -- Rolling averages
        avg(u.unemployment_rate_pct) over (
            partition by ms.country_code 
            order by ms.reference_date 
            rows between 11 preceding and current row
        ) as unemployment_rate_12m_avg,
        
        avg(i.inflation_rate_mom_pct) over (
            partition by ms.country_code 
            order by ms.reference_date 
            rows between 11 preceding and current row
        ) as inflation_rate_12m_avg
        
    from monthly_spine ms
    left join unemployment u 
        on ms.country_code = u.country_code 
        and ms.reference_date = u.reference_date
    left join inflation i 
        on ms.country_code = i.country_code 
        and ms.reference_date = i.reference_date
    left join annual_context ac 
        on ms.country_code = ac.country_code 
        and ms.reference_year = ac.reference_year
)

select * from joined
