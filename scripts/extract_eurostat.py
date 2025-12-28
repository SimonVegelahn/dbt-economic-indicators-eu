#!/usr/bin/env python3
"""
Eurostat Data Extractor
=======================
Pulls economic indicator data from Eurostat's REST API and loads it into DuckDB.

Datasets extracted:
- nama_10_gdp: GDP and main components (annual)
- une_rt_m: Unemployment rate (monthly)
- prc_hicp_mmor: HICP inflation (monthly)
- demo_pjan: Population on 1 January (annual)

Usage:
    python extract_eurostat.py [--full-refresh]
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Eurostat API base URL
EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# Dataset configurations
DATASETS = {
    "nama_10_gdp": {
        "description": "GDP and main components",
        "params": {
            "unit": "CP_MEUR",  # Current prices, million euro
            "na_item": "B1GQ",   # GDP at market prices
            "geo": ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "PL", "EU27_2020"],
        },
        "time_format": "annual",
    },
    "une_rt_m": {
        "description": "Unemployment rate",
        "params": {
            "s_adj": "SA",      # Seasonally adjusted
            "age": "TOTAL",     # Total
            "unit": "PC_ACT",   # Percentage of active population
            "sex": "T",         # Total
            "geo": ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "PL", "EU27_2020"],
        },
        "time_format": "monthly",
    },
    "prc_hicp_mmor": {
        "description": "HICP - monthly data (rate of change)",
        "params": {
            "coicop": "CP00",   # All-items HICP
            "geo": ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "PL", "EU27_2020"],
        },
        "time_format": "monthly",
    },
    "demo_pjan": {
        "description": "Population on 1 January",
        "params": {
            "sex": "T",         # Total
            "age": "TOTAL",     # Total
            "geo": ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "PL", "EU27_2020"],
        },
        "time_format": "annual",
    },
}


def fetch_eurostat_data(dataset_code: str, params: dict) -> dict[str, Any]:
    """
    Fetch data from Eurostat JSON API.
    
    Args:
        dataset_code: Eurostat dataset identifier
        params: Query parameters for filtering
        
    Returns:
        JSON response as dictionary
    """
    url = f"{EUROSTAT_API_BASE}/{dataset_code}"
    
    # Build query parameters
    query_params = {"format": "JSON", "lang": "en"}
    
    for key, value in params.items():
        if isinstance(value, list):
            # Multiple values: repeat the parameter
            for v in value:
                if key in query_params:
                    if isinstance(query_params[key], list):
                        query_params[key].append(v)
                    else:
                        query_params[key] = [query_params[key], v]
                else:
                    query_params[key] = v
        else:
            query_params[key] = value
    
    logger.info(f"Fetching {dataset_code} from Eurostat API...")
    
    try:
        response = requests.get(url, params=query_params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {dataset_code}: {e}")
        raise


def parse_eurostat_json(data: dict, dataset_code: str) -> list[dict]:
    """
    Parse Eurostat JSON-stat format into flat records.
    
    The JSON-stat format uses dimension indices to compress data.
    We need to expand these into readable records.
    """
    records = []
    
    # Get dimension information
    dimensions = data.get("dimension", {})
    dim_ids = data.get("id", [])
    dim_sizes = data.get("size", [])
    values = data.get("value", {})
    
    # Build dimension label lookups
    dim_labels = {}
    for dim_id in dim_ids:
        dim_info = dimensions.get(dim_id, {})
        category = dim_info.get("category", {})
        index = category.get("index", {})
        label = category.get("label", {})
        
        # Map index position to code and label
        dim_labels[dim_id] = {
            "codes": {v: k for k, v in index.items()},
            "labels": label,
        }
    
    # Calculate strides for index computation
    strides = []
    stride = 1
    for size in reversed(dim_sizes):
        strides.insert(0, stride)
        stride *= size
    
    # Iterate through all values
    for flat_idx, value in values.items():
        flat_idx = int(flat_idx)
        record = {
            "dataset_code": dataset_code,
            "value": value,
            "extracted_at": datetime.utcnow().isoformat(),
        }
        
        # Decode each dimension
        remaining = flat_idx
        for i, dim_id in enumerate(dim_ids):
            dim_idx = remaining // strides[i]
            remaining = remaining % strides[i]
            
            code = dim_labels[dim_id]["codes"].get(dim_idx, str(dim_idx))
            record[f"{dim_id}_code"] = code
            record[f"{dim_id}_label"] = dim_labels[dim_id]["labels"].get(code, code)
        
        records.append(record)
    
    logger.info(f"Parsed {len(records)} records from {dataset_code}")
    return records


def load_to_duckdb(records: list[dict], table_name: str, db_path: str, replace: bool = False):
    """
    Load records into DuckDB table.
    
    Args:
        records: List of record dictionaries
        table_name: Target table name
        db_path: Path to DuckDB database file
        replace: If True, replace existing table; otherwise append
    """
    if not records:
        logger.warning(f"No records to load for {table_name}")
        return
    
    # Ensure data directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    try:
        # Create table from records
        if replace:
            conn.execute(f"DROP TABLE IF EXISTS raw_{table_name}")
        
        # Use DuckDB's ability to infer schema from dicts
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS raw_{table_name} AS 
            SELECT * FROM (VALUES {','.join(['(' + ','.join([f"'{v}'" if isinstance(v, str) else str(v) if v is not None else 'NULL' for v in r.values()]) + ')' for r in records[:1]])}) 
            WHERE 1=0
        """)
        
        # Insert records using parameterized query for safety
        columns = list(records[0].keys())
        placeholders = ",".join(["?" for _ in columns])
        
        conn.executemany(
            f"INSERT INTO raw_{table_name} ({','.join(columns)}) VALUES ({placeholders})",
            [tuple(r.get(c) for c in columns) for r in records]
        )
        
        row_count = conn.execute(f"SELECT COUNT(*) FROM raw_{table_name}").fetchone()[0]
        logger.info(f"Loaded {row_count} total rows into raw_{table_name}")
        
    finally:
        conn.close()


def create_raw_tables(db_path: str):
    """
    Create raw tables with proper schema definitions.
    """
    conn = duckdb.connect(db_path)
    
    try:
        # GDP table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_gdp (
                dataset_code VARCHAR,
                value DOUBLE,
                extracted_at TIMESTAMP,
                freq_code VARCHAR,
                freq_label VARCHAR,
                unit_code VARCHAR,
                unit_label VARCHAR,
                na_item_code VARCHAR,
                na_item_label VARCHAR,
                geo_code VARCHAR,
                geo_label VARCHAR,
                time_code VARCHAR,
                time_label VARCHAR
            )
        """)
        
        # Unemployment table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_unemployment (
                dataset_code VARCHAR,
                value DOUBLE,
                extracted_at TIMESTAMP,
                freq_code VARCHAR,
                freq_label VARCHAR,
                s_adj_code VARCHAR,
                s_adj_label VARCHAR,
                age_code VARCHAR,
                age_label VARCHAR,
                unit_code VARCHAR,
                unit_label VARCHAR,
                sex_code VARCHAR,
                sex_label VARCHAR,
                geo_code VARCHAR,
                geo_label VARCHAR,
                time_code VARCHAR,
                time_label VARCHAR
            )
        """)
        
        # Inflation table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_inflation (
                dataset_code VARCHAR,
                value DOUBLE,
                extracted_at TIMESTAMP,
                freq_code VARCHAR,
                freq_label VARCHAR,
                coicop_code VARCHAR,
                coicop_label VARCHAR,
                geo_code VARCHAR,
                geo_label VARCHAR,
                time_code VARCHAR,
                time_label VARCHAR
            )
        """)
        
        # Population table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_population (
                dataset_code VARCHAR,
                value DOUBLE,
                extracted_at TIMESTAMP,
                freq_code VARCHAR,
                freq_label VARCHAR,
                sex_code VARCHAR,
                sex_label VARCHAR,
                age_code VARCHAR,
                age_label VARCHAR,
                geo_code VARCHAR,
                geo_label VARCHAR,
                time_code VARCHAR,
                time_label VARCHAR
            )
        """)
        
        logger.info("Raw tables created successfully")
        
    finally:
        conn.close()


def extract_and_load(db_path: str, full_refresh: bool = False):
    """
    Main extraction pipeline.
    """
    # Ensure data directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Create tables
    create_raw_tables(db_path)
    
    # Table mapping
    table_mapping = {
        "nama_10_gdp": "gdp",
        "une_rt_m": "unemployment",
        "prc_hicp_mmor": "inflation",
        "demo_pjan": "population",
    }
    
    for dataset_code, config in DATASETS.items():
        try:
            # Fetch from API
            data = fetch_eurostat_data(dataset_code, config["params"])
            
            # Parse JSON-stat format
            records = parse_eurostat_json(data, dataset_code)
            
            # Load to DuckDB
            table_name = table_mapping.get(dataset_code, dataset_code)
            load_to_duckdb(records, table_name, db_path, replace=full_refresh)
            
        except Exception as e:
            logger.error(f"Failed to process {dataset_code}: {e}")
            continue
    
    logger.info("Extraction complete!")


def main():
    parser = argparse.ArgumentParser(description="Extract Eurostat data to DuckDB")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Replace existing tables instead of appending",
    )
    parser.add_argument(
        "--db-path",
        default="data/eu_economic.duckdb",
        help="Path to DuckDB database file",
    )
    
    args = parser.parse_args()
    
    extract_and_load(args.db_path, args.full_refresh)


if __name__ == "__main__":
    main()
