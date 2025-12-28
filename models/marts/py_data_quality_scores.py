"""
Python Model: Data Quality Scoring
==================================
This model calculates comprehensive data quality scores
for each country's economic indicators.

Quality Dimensions:
- Completeness: Percentage of non-null values
- Timeliness: How recent is the latest data
- Consistency: Variance in data patterns
- Validity: Values within expected ranges

Output is used for:
- Data quality dashboards
- Alerting on quality degradation
- Prioritizing data remediation efforts
"""

def model(dbt, session):
    """
    Calculate data quality scores for economic indicators.
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # Configuration
    dbt.config(
        materialized='table',
        tags=['python', 'data_quality', 'monitoring']
    )
    
    # Load upstream data
    monthly_df = dbt.ref('fct_economic_indicators').df()
    annual_df = dbt.ref('rpt_annual_economic_summary').df()
    
    # Quality thresholds
    EXPECTED_UNEMPLOYMENT_MIN = 0.0
    EXPECTED_UNEMPLOYMENT_MAX = 30.0
    EXPECTED_INFLATION_MIN = -5.0
    EXPECTED_INFLATION_MAX = 20.0
    TIMELINESS_THRESHOLD_DAYS = 90
    
    def calculate_completeness(df: pd.DataFrame, columns: list) -> dict:
        """Calculate completeness score for specified columns."""
        scores = {}
        for col in columns:
            if col in df.columns:
                non_null_pct = df[col].notna().mean() * 100
                scores[f'{col}_completeness'] = non_null_pct
        
        # Overall completeness
        if scores:
            scores['overall_completeness'] = np.mean(list(scores.values()))
        else:
            scores['overall_completeness'] = 0.0
        
        return scores
    
    def calculate_timeliness(df: pd.DataFrame, date_col: str) -> dict:
        """Calculate timeliness score based on data recency."""
        if date_col not in df.columns:
            return {'timeliness_score': 0.0, 'days_since_latest': None}
        
        latest_date = pd.to_datetime(df[date_col]).max()
        if pd.isna(latest_date):
            return {'timeliness_score': 0.0, 'days_since_latest': None}
        
        days_since_latest = (datetime.now() - latest_date).days
        
        # Score: 100 if within threshold, decreasing after
        if days_since_latest <= TIMELINESS_THRESHOLD_DAYS:
            timeliness_score = 100.0
        else:
            # Decay by 10 points per month after threshold
            months_late = (days_since_latest - TIMELINESS_THRESHOLD_DAYS) / 30
            timeliness_score = max(0, 100 - months_late * 10)
        
        return {
            'timeliness_score': timeliness_score,
            'days_since_latest': days_since_latest,
            'latest_data_date': latest_date
        }
    
    def calculate_validity(df: pd.DataFrame) -> dict:
        """Calculate validity score based on expected value ranges."""
        validity_scores = {}
        
        # Unemployment validity
        if 'unemployment_rate_pct' in df.columns:
            unemp = df['unemployment_rate_pct'].dropna()
            if len(unemp) > 0:
                valid_pct = (
                    (unemp >= EXPECTED_UNEMPLOYMENT_MIN) & 
                    (unemp <= EXPECTED_UNEMPLOYMENT_MAX)
                ).mean() * 100
                validity_scores['unemployment_validity'] = valid_pct
        
        # Inflation validity
        if 'inflation_rate_mom_pct' in df.columns:
            infl = df['inflation_rate_mom_pct'].dropna()
            if len(infl) > 0:
                valid_pct = (
                    (infl >= EXPECTED_INFLATION_MIN) & 
                    (infl <= EXPECTED_INFLATION_MAX)
                ).mean() * 100
                validity_scores['inflation_validity'] = valid_pct
        
        # Overall validity
        if validity_scores:
            validity_scores['overall_validity'] = np.mean(list(validity_scores.values()))
        else:
            validity_scores['overall_validity'] = 100.0
        
        return validity_scores
    
    def calculate_consistency(df: pd.DataFrame) -> dict:
        """Calculate consistency score based on data patterns."""
        consistency_scores = {}
        
        # Check for suspicious patterns (e.g., too many repeated values)
        if 'unemployment_rate_pct' in df.columns:
            unemp = df['unemployment_rate_pct'].dropna()
            if len(unemp) > 10:
                # Count consecutive identical values
                consecutive_same = (unemp.diff() == 0).sum()
                pct_repeated = consecutive_same / len(unemp) * 100
                # High repetition is suspicious
                consistency_scores['unemployment_consistency'] = max(0, 100 - pct_repeated * 2)
        
        if 'inflation_rate_mom_pct' in df.columns:
            infl = df['inflation_rate_mom_pct'].dropna()
            if len(infl) > 10:
                consecutive_same = (infl.diff() == 0).sum()
                pct_repeated = consecutive_same / len(infl) * 100
                consistency_scores['inflation_consistency'] = max(0, 100 - pct_repeated * 2)
        
        if consistency_scores:
            consistency_scores['overall_consistency'] = np.mean(list(consistency_scores.values()))
        else:
            consistency_scores['overall_consistency'] = 100.0
        
        return consistency_scores
    
    # Process each country
    quality_scores = []
    
    for country_code in monthly_df['country_code'].unique():
        country_data = monthly_df[monthly_df['country_code'] == country_code].copy()
        
        # Calculate scores for each dimension
        completeness = calculate_completeness(
            country_data, 
            ['unemployment_rate_pct', 'inflation_rate_mom_pct']
        )
        
        timeliness = calculate_timeliness(country_data, 'reference_date')
        validity = calculate_validity(country_data)
        consistency = calculate_consistency(country_data)
        
        # Calculate overall quality score (weighted average)
        weights = {
            'completeness': 0.30,
            'timeliness': 0.25,
            'validity': 0.25,
            'consistency': 0.20
        }
        
        overall_score = (
            weights['completeness'] * completeness.get('overall_completeness', 0) +
            weights['timeliness'] * timeliness.get('timeliness_score', 0) +
            weights['validity'] * validity.get('overall_validity', 0) +
            weights['consistency'] * consistency.get('overall_consistency', 0)
        )
        
        # Determine quality grade
        if overall_score >= 90:
            quality_grade = 'A'
        elif overall_score >= 80:
            quality_grade = 'B'
        elif overall_score >= 70:
            quality_grade = 'C'
        elif overall_score >= 60:
            quality_grade = 'D'
        else:
            quality_grade = 'F'
        
        # Build record
        record = {
            'country_code': country_code,
            'total_records': len(country_data),
            
            # Completeness
            'completeness_score': completeness.get('overall_completeness', 0),
            'unemployment_completeness': completeness.get('unemployment_rate_pct_completeness', 0),
            'inflation_completeness': completeness.get('inflation_rate_mom_pct_completeness', 0),
            
            # Timeliness
            'timeliness_score': timeliness.get('timeliness_score', 0),
            'days_since_latest_data': timeliness.get('days_since_latest'),
            'latest_data_date': timeliness.get('latest_data_date'),
            
            # Validity
            'validity_score': validity.get('overall_validity', 0),
            'unemployment_validity': validity.get('unemployment_validity', 0),
            'inflation_validity': validity.get('inflation_validity', 0),
            
            # Consistency
            'consistency_score': consistency.get('overall_consistency', 0),
            
            # Overall
            'overall_quality_score': overall_score,
            'quality_grade': quality_grade,
            
            # Metadata
            'scored_at': datetime.now(),
            'scoring_model_version': '1.0.0'
        }
        
        quality_scores.append(record)
    
    # Create DataFrame
    result_df = pd.DataFrame(quality_scores)
    
    # Add quality improvement recommendations
    if len(result_df) > 0:
        result_df['primary_issue'] = result_df.apply(
            lambda row: (
                'completeness' if row['completeness_score'] < 80 else
                'timeliness' if row['timeliness_score'] < 80 else
                'validity' if row['validity_score'] < 80 else
                'consistency' if row['consistency_score'] < 80 else
                'none'
            ),
            axis=1
        )
        
        result_df['requires_attention'] = result_df['overall_quality_score'] < 70
    
    return result_df
