"""
Python Model: Economic Indicator Anomaly Detection
===================================================
This model uses statistical methods to identify anomalous
economic readings that may warrant further investigation.

Use cases:
- Detect data quality issues (implausible values)
- Identify economic shocks (COVID, financial crises)
- Flag outliers for manual review

Methods:
- Z-score based detection (values > 3 std from mean)
- IQR method for robust outlier detection
- Rate-of-change anomalies (sudden spikes/drops)
"""

def model(dbt, session):
    """
    Main dbt Python model function.
    
    Args:
        dbt: dbt context object for accessing refs and configs
        session: Database session (DuckDB connection)
    
    Returns:
        pandas DataFrame with anomaly flags
    """
    import pandas as pd
    import numpy as np
    
    # Configuration
    dbt.config(
        materialized='table',
        tags=['python', 'anomaly_detection', 'data_quality']
    )
    
    # Load upstream model
    monthly_df = dbt.ref('fct_economic_indicators').df()
    
    # Define thresholds
    Z_SCORE_THRESHOLD = 3.0
    IQR_MULTIPLIER = 1.5
    RATE_OF_CHANGE_THRESHOLD = 0.5  # 50% change month-over-month
    
    def calculate_z_scores(series: pd.Series) -> pd.Series:
        """Calculate z-scores for a series."""
        mean = series.mean()
        std = series.std()
        if std == 0 or pd.isna(std):
            return pd.Series(0, index=series.index)
        return (series - mean) / std
    
    def detect_iqr_outliers(series: pd.Series) -> pd.Series:
        """Detect outliers using IQR method."""
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - IQR_MULTIPLIER * iqr
        upper_bound = q3 + IQR_MULTIPLIER * iqr
        return (series < lower_bound) | (series > upper_bound)
    
    def detect_rate_of_change_anomalies(series: pd.Series, threshold: float) -> pd.Series:
        """Detect anomalous rate of change."""
        pct_change = series.pct_change().abs()
        return pct_change > threshold
    
    # Process each country separately for country-specific anomalies
    results = []
    
    for country_code in monthly_df['country_code'].unique():
        country_data = monthly_df[monthly_df['country_code'] == country_code].copy()
        country_data = country_data.sort_values('reference_date')
        
        # Unemployment anomaly detection
        if 'unemployment_rate_pct' in country_data.columns:
            unemp = country_data['unemployment_rate_pct'].dropna()
            if len(unemp) > 10:  # Need sufficient data
                country_data['unemployment_z_score'] = calculate_z_scores(
                    country_data['unemployment_rate_pct']
                )
                country_data['unemployment_iqr_outlier'] = detect_iqr_outliers(
                    country_data['unemployment_rate_pct']
                )
                country_data['unemployment_roc_anomaly'] = detect_rate_of_change_anomalies(
                    country_data['unemployment_rate_pct'],
                    RATE_OF_CHANGE_THRESHOLD
                )
            else:
                country_data['unemployment_z_score'] = np.nan
                country_data['unemployment_iqr_outlier'] = False
                country_data['unemployment_roc_anomaly'] = False
        
        # Inflation anomaly detection
        if 'inflation_rate_mom_pct' in country_data.columns:
            infl = country_data['inflation_rate_mom_pct'].dropna()
            if len(infl) > 10:
                country_data['inflation_z_score'] = calculate_z_scores(
                    country_data['inflation_rate_mom_pct']
                )
                country_data['inflation_iqr_outlier'] = detect_iqr_outliers(
                    country_data['inflation_rate_mom_pct']
                )
                country_data['inflation_roc_anomaly'] = detect_rate_of_change_anomalies(
                    country_data['inflation_rate_mom_pct'],
                    RATE_OF_CHANGE_THRESHOLD
                )
            else:
                country_data['inflation_z_score'] = np.nan
                country_data['inflation_iqr_outlier'] = False
                country_data['inflation_roc_anomaly'] = False
        
        results.append(country_data)
    
    # Combine results
    result_df = pd.concat(results, ignore_index=True)
    
    # Create composite anomaly flag
    result_df['is_unemployment_anomaly'] = (
        (result_df['unemployment_z_score'].abs() > Z_SCORE_THRESHOLD) |
        result_df['unemployment_iqr_outlier'] |
        result_df['unemployment_roc_anomaly']
    ).fillna(False)
    
    result_df['is_inflation_anomaly'] = (
        (result_df['inflation_z_score'].abs() > Z_SCORE_THRESHOLD) |
        result_df['inflation_iqr_outlier'] |
        result_df['inflation_roc_anomaly']
    ).fillna(False)
    
    result_df['is_any_anomaly'] = (
        result_df['is_unemployment_anomaly'] | 
        result_df['is_inflation_anomaly']
    )
    
    # Calculate anomaly severity score (0-100)
    result_df['anomaly_severity_score'] = (
        result_df['unemployment_z_score'].abs().fillna(0).clip(0, 5) * 10 +
        result_df['inflation_z_score'].abs().fillna(0).clip(0, 5) * 10
    ).clip(0, 100)
    
    # Select output columns
    output_columns = [
        'indicator_key',
        'country_code',
        'reference_date',
        'reference_year',
        'reference_month',
        'unemployment_rate_pct',
        'inflation_rate_mom_pct',
        'unemployment_z_score',
        'inflation_z_score',
        'is_unemployment_anomaly',
        'is_inflation_anomaly',
        'is_any_anomaly',
        'anomaly_severity_score'
    ]
    
    # Filter to columns that exist
    output_columns = [c for c in output_columns if c in result_df.columns]
    
    return result_df[output_columns]
