{{
    config(
        materialized='view',
        tags=['intermediate', 'annual']
    )
}}

/*
    Intermediate model: Annual economic metrics by country.
    
    Combines GDP, population, and annualized unemployment/inflation
    into a single annual grain fact table.
    
    This model:
    - Aggregates monthly data to annual level
    - Calculates derived metrics (GDP per capita, avg unemployment)
    - Provides a clean interface for the marts layer
*/

with gdp as (
    select
        country_code,
        reference_year,
        gdp_million_eur,
        _extracted_at
    from {{ ref('stg_eurostat__gdp') }}
),

population as (
    select
        country_code,
        reference_year,
        population_count
    from {{ ref('stg_eurostat__population') }}
),

unemployment_annual as (
    select
        country_code,
        reference_year,
        avg(unemployment_rate_pct) as avg_unemployment_rate_pct,
        min(unemployment_rate_pct) as min_unemployment_rate_pct,
        max(unemployment_rate_pct) as max_unemployment_rate_pct,
        count(*) as unemployment_observations
    from {{ ref('stg_eurostat__unemployment') }}
    group by country_code, reference_year
),

inflation_annual as (
    select
        country_code,
        reference_year,
        -- Sum of monthly rates gives approximate annual inflation
        sum(inflation_rate_mom_pct) as annual_inflation_rate_pct,
        avg(inflation_rate_mom_pct) as avg_monthly_inflation_pct,
        count(*) as inflation_observations
    from {{ ref('stg_eurostat__inflation') }}
    group by country_code, reference_year
),

joined as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['g.country_code', 'g.reference_year']) }} as annual_metrics_key,
        g.country_code,
        g.reference_year,
        
        -- GDP metrics
        g.gdp_million_eur,
        
        -- Population metrics
        p.population_count,
        
        -- Derived: GDP per capita
        case 
            when p.population_count > 0 
            then (g.gdp_million_eur * 1000000.0) / p.population_count 
            else null 
        end as gdp_per_capita_eur,
        
        -- Unemployment metrics (annual aggregates)
        u.avg_unemployment_rate_pct,
        u.min_unemployment_rate_pct,
        u.max_unemployment_rate_pct,
        u.unemployment_observations,
        
        -- Inflation metrics (annual aggregates)
        i.annual_inflation_rate_pct,
        i.avg_monthly_inflation_pct,
        i.inflation_observations,
        
        -- Data quality flags
        case when u.unemployment_observations = 12 then true else false end as has_complete_unemployment_data,
        case when i.inflation_observations = 12 then true else false end as has_complete_inflation_data,
        
        -- Metadata
        g._extracted_at
        
    from gdp g
    left join population p 
        on g.country_code = p.country_code 
        and g.reference_year = p.reference_year
    left join unemployment_annual u 
        on g.country_code = u.country_code 
        and g.reference_year = u.reference_year
    left join inflation_annual i 
        on g.country_code = i.country_code 
        and g.reference_year = i.reference_year
)

select * from joined
