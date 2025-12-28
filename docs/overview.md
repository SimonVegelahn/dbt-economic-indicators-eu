# EU Economic Indicators

{% docs __overview__ %}

## Welcome to the EU Economic Indicators dbt Documentation

This project provides analytics-ready economic indicator data for EU member states, sourced from Eurostat.

### Data Flow

1. **Extraction**: Python script pulls data from Eurostat REST API
2. **Loading**: Raw data loaded into DuckDB
3. **Transformation**: dbt models clean, aggregate, and enrich the data
4. **Consumption**: Final tables ready for dashboards and analysis

### Key Metrics

| Metric | Description | Grain |
|--------|-------------|-------|
| GDP | Gross Domestic Product (million EUR) | Annual |
| Unemployment | Unemployment rate (% active population) | Monthly |
| Inflation | HICP month-over-month change | Monthly |
| Population | Total population count | Annual |

### Model Layers

#### Staging (`models/staging/`)
Raw data with minimal transformation:
- Renaming columns to consistent conventions
- Casting data types
- Adding surrogate keys

#### Intermediate (`models/intermediate/`)
Business logic and aggregations:
- Combining data from multiple sources
- Annual aggregations of monthly data
- Rolling averages and trend calculations

#### Marts (`models/marts/`)
Final tables for consumption:
- **dim_country**: Country dimension with metadata
- **fct_economic_indicators**: Core fact table (incremental)
- **rpt_annual_economic_summary**: Pre-aggregated annual summary

### Getting Started

1. Run `dbt deps` to install packages
2. Run `dbt seed` to load reference data
3. Run `dbt run` to build all models
4. Run `dbt test` to validate data quality

### Questions?

Contact: [Simon Vegelahn](https://www.linkedin.com/in/simonvegelahn)

{% enddocs %}


{% docs country_code %}
ISO 3166-1 alpha-2 country code (e.g., 'DE' for Germany, 'FR' for France).

Special codes:
- `EU27_2020`: European Union aggregate (27 member states as of 2020)

{% enddocs %}


{% docs gdp_million_eur %}
Gross Domestic Product at current market prices, expressed in million EUR.

Source: Eurostat dataset `nama_10_gdp`

{% enddocs %}


{% docs unemployment_rate_pct %}
Unemployment rate as percentage of active population, seasonally adjusted.

- Age group: Total (all ages)
- Sex: Total (all sexes)
- Seasonal adjustment: Yes (SA)

Source: Eurostat dataset `une_rt_m`

{% enddocs %}


{% docs inflation_rate_mom_pct %}
Harmonised Index of Consumer Prices (HICP), month-over-month rate of change.

- Coverage: All-items (COICOP: CP00)
- Calculation: (Current month index / Previous month index - 1) * 100

Source: Eurostat dataset `prc_hicp_mmor`

{% enddocs %}
