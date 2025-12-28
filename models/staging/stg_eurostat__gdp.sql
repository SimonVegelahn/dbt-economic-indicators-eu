{{
    config(
        materialized='view',
        tags=['staging', 'gdp']
    )
}}

/*
    Staging model for GDP data from Eurostat.
    
    Transformations applied:
    - Rename columns to consistent naming convention
    - Cast time_code to proper date type
    - Filter to valid records only
    - Add surrogate key for downstream joins
*/

with source as (
    select * from {{ source('eurostat_raw', 'raw_gdp') }}
),

renamed as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['geo_code', 'time_code']) }} as gdp_key,
        
        -- Dimensions
        geo_code as country_code,
        geo_label as country_name,
        
        -- Time dimension
        time_code as year_code,
        cast(time_code as integer) as reference_year,
        make_date(cast(time_code as integer), 1, 1) as reference_date,
        
        -- Measures
        value as gdp_million_eur,
        
        -- Metadata
        unit_code,
        unit_label as unit_description,
        na_item_code as national_accounts_item_code,
        na_item_label as national_accounts_item,
        dataset_code as source_dataset,
        extracted_at as _extracted_at
        
    from source
    where value is not null
      and time_code is not null
      and geo_code is not null
)

select * from renamed
