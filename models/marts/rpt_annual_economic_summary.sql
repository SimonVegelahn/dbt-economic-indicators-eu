{{
    config(
        materialized='table',
        tags=['marts', 'summary'],
        contract={'enforced': true}
    )
}}

/*
    Summary table: Annual economic performance by country.
    
    Pre-aggregated annual metrics with rankings and comparisons
    for executive dashboards and high-level reporting.
    
    Grain: One row per country per year
*/

with annual_metrics as (
    select * from {{ ref('int_country_annual_metrics') }}
),

country_dim as (
    select * from {{ ref('dim_country') }}
),

-- Calculate EU-wide aggregates for comparison
eu_aggregates as (
    select
        reference_year,
        sum(gdp_million_eur) as eu_total_gdp_million_eur,
        sum(population_count) as eu_total_population,
        avg(avg_unemployment_rate_pct) as eu_avg_unemployment_rate,
        avg(annual_inflation_rate_pct) as eu_avg_inflation_rate
    from annual_metrics
    where country_code != 'EU27_2020'  -- Exclude EU aggregate to avoid double counting
    group by reference_year
),

ranked as (
    select
        am.*,
        
        -- Rankings within each year
        rank() over (partition by am.reference_year order by am.gdp_million_eur desc) as gdp_rank,
        rank() over (partition by am.reference_year order by am.gdp_per_capita_eur desc) as gdp_per_capita_rank,
        rank() over (partition by am.reference_year order by am.avg_unemployment_rate_pct asc) as unemployment_rank,
        rank() over (partition by am.reference_year order by am.annual_inflation_rate_pct asc) as inflation_rank,
        
        -- Year-over-year changes
        lag(am.gdp_million_eur) over (partition by am.country_code order by am.reference_year) as prev_year_gdp,
        lag(am.avg_unemployment_rate_pct) over (partition by am.country_code order by am.reference_year) as prev_year_unemployment,
        lag(am.annual_inflation_rate_pct) over (partition by am.country_code order by am.reference_year) as prev_year_inflation
        
    from annual_metrics am
    where am.country_code != 'EU27_2020'  -- Exclude EU aggregate for country-level analysis
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['r.country_code', 'r.reference_year']) }} as summary_key,
        r.country_code,
        cd.country_key,
        r.reference_year,
        
        -- Absolute metrics
        r.gdp_million_eur,
        r.population_count,
        r.gdp_per_capita_eur,
        r.avg_unemployment_rate_pct,
        r.annual_inflation_rate_pct,
        
        -- Rankings
        r.gdp_rank,
        r.gdp_per_capita_rank,
        r.unemployment_rank,
        r.inflation_rank,
        
        -- YoY changes
        case 
            when r.prev_year_gdp > 0 
            then ((r.gdp_million_eur - r.prev_year_gdp) / r.prev_year_gdp) * 100 
            else null 
        end as gdp_yoy_growth_pct,
        r.avg_unemployment_rate_pct - r.prev_year_unemployment as unemployment_yoy_change_pp,
        r.annual_inflation_rate_pct - r.prev_year_inflation as inflation_yoy_change_pp,
        
        -- Share of EU total
        case 
            when eu.eu_total_gdp_million_eur > 0 
            then (r.gdp_million_eur / eu.eu_total_gdp_million_eur) * 100 
            else null 
        end as share_of_eu_gdp_pct,
        case 
            when eu.eu_total_population > 0 
            then (r.population_count::float / eu.eu_total_population) * 100 
            else null 
        end as share_of_eu_population_pct,
        
        -- Comparison to EU average
        r.gdp_per_capita_eur - (eu.eu_total_gdp_million_eur * 1000000.0 / eu.eu_total_population) as gdp_per_capita_vs_eu_avg,
        r.avg_unemployment_rate_pct - eu.eu_avg_unemployment_rate as unemployment_vs_eu_avg_pp,
        r.annual_inflation_rate_pct - eu.eu_avg_inflation_rate as inflation_vs_eu_avg_pp,
        
        -- Data quality
        r.has_complete_unemployment_data,
        r.has_complete_inflation_data,
        
        -- Country attributes (denormalized for convenience)
        cd.country_name,
        cd.eu_status,
        cd.eurozone_member,
        cd.region,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from ranked r
    left join country_dim cd on r.country_code = cd.country_code
    left join eu_aggregates eu on r.reference_year = eu.reference_year
)

select * from final
