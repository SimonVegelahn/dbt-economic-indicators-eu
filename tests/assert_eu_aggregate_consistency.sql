/*
    Singular test: Validate EU aggregate equals sum of member states.
    
    This test checks that the EU27_2020 aggregate GDP approximately equals
    the sum of individual country GDPs for each year.
    
    A 5% tolerance is allowed due to:
    - Rounding differences
    - Timing of country data submissions
    - Statistical adjustments
*/

with country_totals as (
    select
        reference_year,
        sum(gdp_million_eur) as summed_gdp
    from {{ ref('stg_eurostat__gdp') }}
    where country_code != 'EU27_2020'
    group by reference_year
),

eu_aggregates as (
    select
        reference_year,
        gdp_million_eur as eu_reported_gdp
    from {{ ref('stg_eurostat__gdp') }}
    where country_code = 'EU27_2020'
),

comparison as (
    select
        ct.reference_year,
        ct.summed_gdp,
        ea.eu_reported_gdp,
        abs(ct.summed_gdp - ea.eu_reported_gdp) as absolute_difference,
        abs(ct.summed_gdp - ea.eu_reported_gdp) / ea.eu_reported_gdp * 100 as percentage_difference
    from country_totals ct
    inner join eu_aggregates ea on ct.reference_year = ea.reference_year
)

-- Return rows where difference exceeds 5% tolerance
select *
from comparison
where percentage_difference > 5
