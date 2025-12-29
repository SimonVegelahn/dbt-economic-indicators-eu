{{
    config(
        materialized='view',
        tags=['staging', 'inflation']
    )
}}

/*
    Staging model for HICP inflation data from Eurostat.
    
    Transformations applied:
    - Rename columns to consistent naming convention
    - Parse monthly time_code to date
    - Filter to valid records only
    - Add surrogate key for downstream joins
*/

with source as (
    select * from {{ source('eurostat_raw', 'raw_inflation') }}
),

renamed as (
    select
        -- Surrogate key
        {{ generate_surrogate_key(['geo_code', 'time_code']) }} as inflation_key,
        
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
        
        -- Measures (month-over-month rate of change)
        value as inflation_rate_mom_pct,
        
        -- Metadata
        coicop_code,
        coicop_label as coicop_category,
        dataset_code as source_dataset,
        extracted_at as _extracted_at
        
    from source
    where value is not null
      and time_code is not null
      and geo_code is not null
      and length(time_code) >= 7
)

select * from renamed
