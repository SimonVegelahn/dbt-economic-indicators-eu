{{
    config(
        materialized='view',
        tags=['staging', 'unemployment']
    )
}}

/*
    Staging model for unemployment rate data from Eurostat.
    
    Transformations applied:
    - Rename columns to consistent naming convention
    - Parse monthly time_code to date
    - Filter to valid records only
    - Add surrogate key for downstream joins
*/

with source as (
    select * from {{ source('eurostat_raw', 'raw_unemployment') }}
),

renamed as (
    select
        -- Surrogate key
        {{ generate_surrogate_key(['geo_code', 'time_code']) }} as unemployment_key,
        
        -- Dimensions
        geo_code as country_code,
        geo_label as country_name,
        
        -- Time dimension (format: YYYY-MM)
        time_code as period_code,
        cast(substr(time_code, 1, 4) as integer) as reference_year,
        cast(substr(time_code, 6, 2) as integer) as reference_month,
        make_date(
            cast(substr(time_code, 1, 4) as integer),
            cast(substr(time_code, 6, 2) as integer),
            1
        ) as reference_date,
        
        -- Measures
        value as unemployment_rate_pct,
        
        -- Metadata
        s_adj_code as seasonal_adjustment_code,
        s_adj_label as seasonal_adjustment,
        age_code,
        age_label as age_group,
        sex_code,
        sex_label as sex,
        unit_code,
        unit_label as unit_description,
        dataset_code as source_dataset,
        extracted_at as _extracted_at
        
    from source
    where value is not null
      and time_code is not null
      and geo_code is not null
      -- Filter to ensure we have valid monthly format
      and length(time_code) >= 7
)

select * from renamed
