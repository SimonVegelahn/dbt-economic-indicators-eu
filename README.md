# EU Economic Indicators - dbt Project

A production-grade dbt project demonstrating modern data engineering practices using Eurostat economic data.

[![dbt](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.9+-yellow.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

This project extracts economic indicators from the [Eurostat REST API](https://ec.europa.eu/eurostat/web/main/data/web-services) and transforms them into analytics-ready datasets using dbt (data build tool) with DuckDB as the warehouse.

### Data Sources

| Dataset | Description | Grain | Update Frequency |
|---------|-------------|-------|------------------|
| `nama_10_gdp` | GDP and main components | Annual | Quarterly |
| `une_rt_m` | Unemployment rate | Monthly | Monthly |
| `prc_hicp_mmor` | HICP inflation | Monthly | Monthly |
| `demo_pjan` | Population | Annual | Annual |

### Countries Covered

Core EU economies: Germany (DE), France (FR), Italy (IT), Spain (ES), Netherlands (NL), Belgium (BE), Austria (AT), Poland (PL), plus EU27 aggregate.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Eurostat API  │────▶│     DuckDB      │────▶│   dbt Models    │
│   (REST JSON)   │     │   (raw tables)  │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                        ┌───────────────────────────────┼───────────────────────────────┐
                        │                               │                               │
                        ▼                               ▼                               ▼
                ┌───────────────┐              ┌───────────────┐              ┌───────────────┐
                │    Staging    │              │ Intermediate  │              │     Marts     │
                │               │              │               │              │               │
                │ • stg_gdp     │─────────────▶│ • int_annual  │─────────────▶│ • dim_country │
                │ • stg_unemp   │              │ • int_monthly │              │ • fct_econ    │
                │ • stg_infl    │              │               │              │ • rpt_summary │
                │ • stg_pop     │              │               │              │               │
                └───────────────┘              └───────────────┘              └───────────────┘
```

## Project Structure

```
eu_economic_indicators/
├── models/
│   ├── staging/           # 1:1 with source tables, light transformations
│   │   ├── _sources.yml   # Source definitions with freshness checks
│   │   ├── _staging__models.yml
│   │   ├── stg_eurostat__gdp.sql
│   │   ├── stg_eurostat__unemployment.sql
│   │   ├── stg_eurostat__inflation.sql
│   │   └── stg_eurostat__population.sql
│   │
│   ├── intermediate/      # Business logic, aggregations
│   │   ├── _intermediate__models.yml
│   │   ├── int_country_annual_metrics.sql
│   │   └── int_country_monthly_indicators.sql
│   │
│   └── marts/             # Final tables for consumption
│       ├── _marts__models.yml
│       ├── dim_country.sql
│       ├── fct_economic_indicators.sql      # Incremental
│       └── rpt_annual_economic_summary.sql
│
├── seeds/
│   └── country_metadata.csv   # Reference data for countries
│
├── snapshots/
│   └── snap_gdp_history.sql   # SCD Type 2 for GDP revisions
│
├── macros/
│   ├── calculations.sql       # YoY change, rolling avg, etc.
│   └── data_quality.sql       # Custom tests, schema naming
│
├── tests/
│   └── assert_eu_aggregate_consistency.sql  # Singular test
│
├── scripts/
│   └── extract_eurostat.py    # Python extraction script
│
├── dbt_project.yml
├── profiles.yml
├── packages.yml
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.9+
- dbt-core 1.7+
- dbt-duckdb 1.7+

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/eu_economic_indicators.git
cd eu_economic_indicators

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install dbt-core dbt-duckdb requests

# Install dbt packages
dbt deps
```

### Extract Data

```bash
# Full extraction (creates/replaces tables)
python scripts/extract_eurostat.py --full-refresh

# Incremental extraction (appends new data)
python scripts/extract_eurostat.py
```

### Run dbt

```bash
# Seed reference data
dbt seed

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Key Features Demonstrated

### 1. Layered Architecture
- **Staging**: Thin transformations, source-aligned
- **Intermediate**: Business logic, aggregations
- **Marts**: Consumption-ready, denormalized

### 2. Incremental Models
`fct_economic_indicators` uses incremental materialization for efficient processing:
```sql
{{
    config(
        materialized='incremental',
        unique_key='indicator_key',
        on_schema_change='append_new_columns'
    )
}}
```

### 3. Snapshots (SCD Type 2)
`snap_gdp_history` tracks GDP data revisions over time using the `check` strategy.

### 4. Testing Strategy
- **Generic tests**: not_null, unique, relationships, accepted_range
- **Singular tests**: EU aggregate consistency validation
- **Source freshness**: Automated staleness monitoring

### 5. Documentation
- Column-level descriptions
- Model dependencies via `ref()`
- Generated lineage graphs

### 6. Macros
- Reusable calculation macros (YoY change, rolling averages)
- Custom schema naming for dev/prod environments
- Data quality helpers

## Example Queries

### GDP Growth Ranking (2023)
```sql
select
    country_name,
    gdp_million_eur,
    gdp_yoy_growth_pct,
    gdp_rank
from marts.rpt_annual_economic_summary
where reference_year = 2023
order by gdp_rank;
```

### Unemployment Trend Analysis
```sql
select
    country_code,
    reference_date,
    unemployment_rate_pct,
    unemployment_rate_12m_avg,
    unemployment_yoy_change
from marts.fct_economic_indicators
where country_code = 'DE'
  and reference_date >= '2020-01-01'
order by reference_date;
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DBT_TARGET` | Target environment (dev/prod) | `dev` |
| `DB_PATH` | Path to DuckDB file | `data/eu_economic.duckdb` |

### Variables in `dbt_project.yml`

```yaml
vars:
  start_year: 2010
  end_year: 2024
  focus_countries: ['DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'AT', 'PL']
```

## Data Quality

### Source Freshness

Sources are configured with freshness checks:
```yaml
freshness:
  warn_after: {count: 7, period: day}
  error_after: {count: 30, period: day}
```

Run freshness check:
```bash
dbt source freshness
```

### Test Coverage

| Layer | Tests |
|-------|-------|
| Staging | not_null, unique on keys, accepted_range |
| Intermediate | unique composite keys |
| Marts | relationships, custom business rules |

## Contributing

1. Create a feature branch
2. Make changes
3. Run `dbt test` to ensure tests pass
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

**Simon Vegelahn**  
Business Informatics Student | Data Engineer  
[LinkedIn](https://www.linkedin.com/in/simonvegelahn) | [GitHub](https://github.com/SimonVegelahn)

---

*Built with dbt + DuckDB + Eurostat Open Data*
