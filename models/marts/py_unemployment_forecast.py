"""
Python Model: Unemployment Rate Forecasting
============================================
This model generates short-term forecasts for unemployment rates
using multiple statistical methods.

Methods:
- Exponential Smoothing (Holt-Winters)
- Linear Trend Extrapolation
- Simple Moving Average

The model generates 6-month ahead forecasts for each country
and provides confidence intervals.

Note: This is a demonstration of ML in dbt. Production forecasting
would typically use more sophisticated models (ARIMA, Prophet, etc.)
and external ML platforms.
"""

def model(dbt, session):
    """
    Generate unemployment rate forecasts using statistical methods.
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # Configuration
    dbt.config(
        materialized='table',
        tags=['python', 'forecasting', 'ml']
    )
    
    # Load upstream data
    monthly_df = dbt.ref('fct_economic_indicators').df()
    
    # Forecast parameters
    FORECAST_HORIZON = 6  # months
    MIN_HISTORY_MONTHS = 24  # Minimum data points required
    
    def exponential_smoothing(series: pd.Series, alpha: float = 0.3) -> float:
        """
        Simple exponential smoothing for next period forecast.
        """
        if len(series) < 2:
            return series.iloc[-1] if len(series) > 0 else np.nan
        
        result = series.iloc[0]
        for value in series.iloc[1:]:
            result = alpha * value + (1 - alpha) * result
        
        return result
    
    def holt_linear_trend(series: pd.Series, alpha: float = 0.3, beta: float = 0.1) -> tuple:
        """
        Holt's linear trend method.
        Returns (level, trend) for forecasting.
        """
        if len(series) < 3:
            return series.iloc[-1] if len(series) > 0 else np.nan, 0
        
        # Initialize
        level = series.iloc[0]
        trend = series.iloc[1] - series.iloc[0]
        
        # Update
        for value in series.iloc[1:]:
            last_level = level
            level = alpha * value + (1 - alpha) * (level + trend)
            trend = beta * (level - last_level) + (1 - beta) * trend
        
        return level, trend
    
    def linear_regression_forecast(series: pd.Series, periods_ahead: int) -> list:
        """
        Simple linear regression for trend extrapolation.
        """
        if len(series) < 3:
            return [series.iloc[-1] if len(series) > 0 else np.nan] * periods_ahead
        
        x = np.arange(len(series))
        y = series.values
        
        # Remove NaN
        mask = ~np.isnan(y)
        x = x[mask]
        y = y[mask]
        
        if len(x) < 3:
            return [series.dropna().iloc[-1] if len(series.dropna()) > 0 else np.nan] * periods_ahead
        
        # Fit linear regression
        n = len(x)
        x_mean = x.mean()
        y_mean = y.mean()
        
        slope = np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean) ** 2)
        intercept = y_mean - slope * x_mean
        
        # Forecast
        future_x = np.arange(len(series), len(series) + periods_ahead)
        forecasts = intercept + slope * future_x
        
        return forecasts.tolist()
    
    def calculate_prediction_interval(historical_values: pd.Series, forecast: float, confidence: float = 0.95) -> tuple:
        """
        Calculate prediction interval based on historical volatility.
        """
        if len(historical_values) < 5:
            return forecast - 1, forecast + 1
        
        # Use standard deviation of recent changes
        changes = historical_values.diff().dropna()
        std = changes.std()
        
        # Z-score for confidence level (approximately)
        z = 1.96 if confidence == 0.95 else 1.645
        
        margin = z * std * np.sqrt(1 + 1/len(historical_values))
        
        return forecast - margin, forecast + margin
    
    # Process each country
    forecasts = []
    
    for country_code in monthly_df['country_code'].unique():
        country_data = monthly_df[monthly_df['country_code'] == country_code].copy()
        country_data = country_data.sort_values('reference_date')
        
        # Get unemployment series
        unemp_series = country_data.set_index('reference_date')['unemployment_rate_pct'].dropna()
        
        if len(unemp_series) < MIN_HISTORY_MONTHS:
            continue
        
        # Get last date for forecast starting point
        last_date = unemp_series.index.max()
        last_value = unemp_series.iloc[-1]
        
        # Generate forecasts using different methods
        # 1. Exponential Smoothing
        es_forecast = exponential_smoothing(unemp_series)
        
        # 2. Holt's Linear Trend
        level, trend = holt_linear_trend(unemp_series)
        
        # 3. Linear Regression
        lr_forecasts = linear_regression_forecast(unemp_series, FORECAST_HORIZON)
        
        # Generate forecast rows
        for i in range(FORECAST_HORIZON):
            # Calculate forecast date
            forecast_date = last_date + pd.DateOffset(months=i+1)
            forecast_date = forecast_date.replace(day=1)  # First of month
            
            # Holt forecast for this horizon
            holt_forecast = level + (i + 1) * trend
            
            # Ensemble forecast (average of methods)
            methods_forecasts = [
                es_forecast + i * trend,  # ES with trend adjustment
                holt_forecast,
                lr_forecasts[i]
            ]
            ensemble_forecast = np.nanmean(methods_forecasts)
            
            # Prediction interval
            lower, upper = calculate_prediction_interval(
                unemp_series, 
                ensemble_forecast
            )
            
            forecasts.append({
                'country_code': country_code,
                'forecast_date': forecast_date,
                'forecast_horizon_months': i + 1,
                'last_actual_date': last_date,
                'last_actual_value': last_value,
                'forecast_exp_smoothing': es_forecast + i * trend,
                'forecast_holt': holt_forecast,
                'forecast_linear_reg': lr_forecasts[i],
                'forecast_ensemble': ensemble_forecast,
                'prediction_interval_lower': lower,
                'prediction_interval_upper': upper,
                'forecast_generated_at': datetime.now(),
                'model_version': '1.0.0',
                'min_training_samples': len(unemp_series)
            })
    
    # Create DataFrame
    result_df = pd.DataFrame(forecasts)
    
    # Add uncertainty quantification
    if len(result_df) > 0:
        result_df['prediction_interval_width'] = (
            result_df['prediction_interval_upper'] - 
            result_df['prediction_interval_lower']
        )
        result_df['forecast_confidence'] = np.where(
            result_df['prediction_interval_width'] < 1.0, 'high',
            np.where(result_df['prediction_interval_width'] < 2.0, 'medium', 'low')
        )
    
    return result_df
