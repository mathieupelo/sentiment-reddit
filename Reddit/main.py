#!/usr/bin/env python3
"""
Main entry point for sentiment-reddit signal processing.

Generates Reddit sentiment signals for gaming company stocks over the last 3 years.
"""

import sys
from pathlib import Path
from datetime import date, timedelta
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.calculate_signals import calculate_signals_optimized

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Main function - generates Reddit sentiment signals for gaming stocks over last 3 years.
    """
    print("=" * 80)
    print("REDDIT SENTIMENT ANALYSIS FOR GAMING STOCKS")
    print("=" * 80)
    print()
    
    # Define gaming company tickers
    gaming_tickers = [
        'EA', 'TTWO', 'NTES', 'RBLX', 'MSFT', 'SONY', 
        'WBD', 'NCBDY', 'GDEV', 'OTGLF', 'SNAL', 'GRVY'
    ]
    
    # Set date range for last 3 years
    end_date = date.today()
    start_date = end_date - timedelta(days=700)  # 700 days
    
    print(f"Analyzing {len(gaming_tickers)} gaming company tickers:")
    print(f"Tickers: {', '.join(gaming_tickers)}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total days: {(end_date - start_date).days}")
    print()
    
    # Configuration
    lookback_days = 30  # Look back 30 days for Reddit posts for each signal date
    
    print("Configuration:")
    print(f"- Lookback days: {lookback_days}")
    print(f"- Estimated runtime: 2-3 minutes (optimized cache-first approach)")
    print()
    
    # Confirm before proceeding
    response = input("Proceed with sentiment analysis? (y/N): ").strip().lower()
    if response != 'y':
        print("Analysis cancelled.")
        return 0
    
    print()
    print("Starting sentiment analysis...")
    print("=" * 80)
    
    try:
        # Generate sentiment signals using optimized cache-first approach
        logger.info(f"Generating sentiment signals for {len(gaming_tickers)} tickers from {start_date} to {end_date}")
        result = calculate_signals_optimized(
            tickers=gaming_tickers,
            start_date=start_date,
            end_date=end_date
        )
        
        # Extract signals dataframe from result
        if isinstance(result, dict) and 'signals_df' in result:
            signals_df = result['signals_df']
        elif hasattr(result, 'signals_df'):
            signals_df = result.signals_df
        else:
            # If result is already a dataframe
            signals_df = result
        
        # Display summary
        print()
        print("=" * 80)
        print("ANALYSIS COMPLETE - SUMMARY")
        print("=" * 80)
        print(f"Total signals generated: {len(signals_df)}")
        print(f"Average sentiment: {signals_df['value'].mean():.3f}")
        print(f"Average confidence: {signals_df['confidence'].mean():.3f}")
        
        signals_with_data = len(signals_df[signals_df['posts_analyzed'] > 0])
        total_posts = signals_df['posts_analyzed'].sum()
        print(f"Signals with data: {signals_with_data} / {len(signals_df)} ({signals_with_data/len(signals_df):.1%})")
        print(f"Total posts analyzed: {total_posts}")
        print(f"Average posts per signal: {total_posts/len(signals_df):.1f}")
        
        # Ticker breakdown
        print()
        print("TICKER BREAKDOWN:")
        print("-" * 40)
        for ticker in gaming_tickers:
            ticker_data = signals_df[signals_df['ticker'] == ticker]
            ticker_with_data = len(ticker_data[ticker_data['posts_analyzed'] > 0])
            avg_sentiment = ticker_data['value'].mean()
            avg_confidence = ticker_data['confidence'].mean()
            
            print(f"{ticker:6}: {ticker_with_data:4}/{len(ticker_data):4} signals with data")
            print(f"       Overall sentiment: {avg_sentiment:7.3f}, confidence: {avg_confidence:.3f}")
        
        # Export results
        output_file = f"data/gaming_sentiment_3years_{start_date}_{end_date}.csv"
        
        # Create data directory if it doesn't exist
        from pathlib import Path
        Path("data").mkdir(exist_ok=True)
        
        # Export to CSV
        try:
            signals_df.to_csv(output_file, index=False)
            success = True
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            success = False
        
        if success:
            print()
            print(f"SUCCESS: Signals exported to: {output_file}")
            print()
            print("EXPORT FORMAT:")
            print("-" * 30)
            print("Columns: asof_date, ticker, signal_name, value, confidence, posts_analyzed, calculation_method, search_terms")
            print("Format: CSV with point-in-time sentiment scores")
            print("Ready for: External backtesting software integration")
        else:
            print("ERROR: Export failed")
            return 1
        
        print()
        print("=" * 80)
        print("SENTIMENT ANALYSIS COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Error during sentiment analysis: {e}")
        print(f"ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
