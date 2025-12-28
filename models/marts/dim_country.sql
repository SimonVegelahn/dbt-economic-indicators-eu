{{
    config(
        materialized='table',
        tags=['marts', 'dimensions'],
        contract={'enforced': true}
    )
}}

/*
    Dimension table: Countries.
    
    Contains reference data for EU member states including
    metadata from seeds and derived attributes.
*/

with country_seed as (
    select * from {{ ref('country_metadata') }}
),

-- Get latest country names from staging data
country_names as (
    select distinct
        country_code,
        first_value(country_name) over (
            partition by country_code 
            order by reference_year desc
        ) as country_name_eurostat
    from {{ ref('stg_eurostat__gdp') }}
),

-- Get data availability summary
data_availability as (
    select
        country_code,
        min(reference_year) as earliest_gdp_year,
        max(reference_year) as latest_gdp_year,
        count(distinct reference_year) as years_of_gdp_data
    from {{ ref('stg_eurostat__gdp') }}
    group by country_code
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['cs.country_code']) }} as country_key,
        
        -- Natural key
        cs.country_code,
        
        -- Descriptive attributes
        coalesce(cs.country_name, cn.country_name_eurostat) as country_name,
        cs.eu_member_since,
        cs.eurozone_member,
        cs.region,
        cs.subregion,
        
        -- Derived attributes
        case 
            when cs.eurozone_member then 'Eurozone'
            when cs.eu_member_since is not null then 'EU (non-Euro)'
            else 'Non-EU'
        end as eu_status,
        
        -- Data availability
        da.earliest_gdp_year,
        da.latest_gdp_year,
        da.years_of_gdp_data,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from country_seed cs
    left join country_names cn on cs.country_code = cn.country_code
    left join data_availability da on cs.country_code = da.country_code
)

select * from final
