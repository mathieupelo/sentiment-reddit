"""
Simple sentiment analysis service for Reddit data.

This module provides simplified sentiment analysis functionality
that uses only ticker symbols and company names for backtesting.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import sys
from pathlib import Path

# Add utils to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.calculate_signals import (
    calculate_signals_optimized,
    finbert_analyzer
)

logger = logging.getLogger(__name__)


def calculate_simple_sentiment(tickers: List[str], 
                              start_date: date, 
                              end_date: date, 
                              lookback_days: int = 30) -> pd.DataFrame:
    """
    Calculate simple sentiment signals for backtesting.
    
    Args:
        tickers: List of stock tickers
        start_date: Start date for signals
        end_date: End date for signals
        lookback_days: Lookback window for posts
        
    Returns:
        DataFrame with sentiment signals
    """
    logger.info(f"Calculating simple sentiment for {len(tickers)} tickers")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Lookback days: {lookback_days}")
    
    # Use the optimized signal calculation
    result = calculate_signals_optimized(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        use_cache=True
    )
    
    # Extract signals dataframe
    signals_df = result['signals_df']
    
    # Add search terms metadata
    signals_df['search_terms'] = signals_df['ticker'].apply(
        lambda x: f"{x.lower()}, {', '.join([name.lower() for name in get_company_names(x)])}"
    )
    
    logger.info(f"Generated {len(signals_df)} simple sentiment signals")
    
    return signals_df


def get_company_names(ticker: str) -> List[str]:
    """Get company names for a ticker."""
    from utils.calculate_signals import TICKER_GAME_MAPPING
    return TICKER_GAME_MAPPING.get(ticker, [ticker])


def export_signals_for_backtesting(signals_df: pd.DataFrame, output_file: str) -> bool:
    """
    Export signals to CSV format suitable for backtesting software.
    
    Args:
        signals_df: DataFrame with sentiment signals
        output_file: Output CSV file path
        
    Returns:
        True if export successful, False otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare data for export
        export_df = signals_df.copy()
        
        # Flatten metadata for CSV export
        if 'metadata' in export_df.columns:
            export_df['posts_analyzed'] = export_df['metadata'].apply(
                lambda x: x.get('posts_analyzed', 0) if isinstance(x, dict) else 0
            )
            export_df['calculation_method'] = export_df['metadata'].apply(
                lambda x: x.get('calculation_method', 'unknown') if isinstance(x, dict) else 'unknown'
            )
        else:
            export_df['posts_analyzed'] = 0
            export_df['calculation_method'] = 'unknown'
        
        # Select and order columns for export
        export_columns = [
            'asof_date', 'ticker', 'signal_name', 'value', 'confidence',
            'posts_analyzed', 'calculation_method', 'search_terms'
        ]
        
        # Ensure all required columns exist
        for col in export_columns:
            if col not in export_df.columns:
                export_df[col] = ''
        
        export_df = export_df[export_columns]
        
        # Export to CSV
        export_df.to_csv(output_file, index=False)
        
        logger.info(f"Successfully exported {len(export_df)} signals to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting signals: {e}")
        return False


def get_sentiment_summary(signals_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get summary statistics for sentiment signals.
    
    Args:
        signals_df: DataFrame with sentiment signals
        
    Returns:
        Dictionary with summary statistics
    """
    summary = {
        'total_signals': len(signals_df),
        'unique_tickers': signals_df['ticker'].nunique(),
        'date_range': {
            'start': signals_df['asof_date'].min(),
            'end': signals_df['asof_date'].max()
        },
        'sentiment_stats': {
            'mean': signals_df['value'].mean(),
            'std': signals_df['value'].std(),
            'min': signals_df['value'].min(),
            'max': signals_df['value'].max()
        },
        'confidence_stats': {
            'mean': signals_df['confidence'].mean(),
            'std': signals_df['confidence'].std(),
            'min': signals_df['confidence'].min(),
            'max': signals_df['confidence'].max()
        },
        'data_quality': {
            'signals_with_data': len(signals_df[signals_df['posts_analyzed'] > 0]),
            'signals_fallback': len(signals_df[signals_df['posts_analyzed'] == 0]),
            'total_posts': signals_df['posts_analyzed'].sum()
        }
    }
    
    return summary
