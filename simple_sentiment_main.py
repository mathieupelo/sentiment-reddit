#!/usr/bin/env python3
"""
Main script for simple Reddit sentiment analysis.

This script generates point-in-time sentiment signals using only ticker symbols
and company names, suitable for integration with external backtesting software.
"""

import argparse
import logging
from datetime import date, datetime, timedelta
from typing import List
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from services.simple_sentiment import calculate_simple_sentiment, export_signals_for_backtesting

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_sentiment_signals(args):
    """Generate simple sentiment signals for backtesting."""
    
    # Parse dates
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = date.today() - timedelta(days=30)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = date.today()
    
    # Parse tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(',')]
    else:
        tickers = ['EA', 'TTWO', 'NTES', 'RBLX', 'MSFT', 'SONY', 'WBD', 'NCBDY', 'GDEV', 'OTGLF', 'SNAL', 'GRVY']
    
    logger.info(f"Generating simple sentiment signals for {tickers}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Lookback days: {args.lookback_days}")
    
    # Calculate simple sentiment
    signals_df = calculate_simple_sentiment(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        lookback_days=args.lookback_days
    )
    
    # Display summary
    print("=" * 60)
    print("SIMPLE REDDIT SENTIMENT ANALYSIS RESULTS")
    print("=" * 60)
    print(f"Generated signals: {len(signals_df)}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Tickers analyzed: {', '.join(tickers)}")
    print(f"Lookback days: {args.lookback_days}")
    print("")
    
    # Summary statistics
    print("SUMMARY STATISTICS")
    print("-" * 30)
    print(f"Average sentiment: {signals_df['value'].mean():.3f}")
    print(f"Sentiment std dev: {signals_df['value'].std():.3f}")
    print(f"Min sentiment: {signals_df['value'].min():.3f}")
    print(f"Max sentiment: {signals_df['value'].max():.3f}")
    print(f"Average confidence: {signals_df['confidence'].mean():.3f}")
    print("")
    
    # Data quality metrics
    signals_with_data = len(signals_df[signals_df['metadata'].apply(lambda x: x.get('posts_analyzed', 0) > 0)])
    total_posts = signals_df['metadata'].apply(lambda x: x.get('posts_analyzed', 0)).sum()
    
    print("DATA QUALITY METRICS")
    print("-" * 30)
    print(f"Signals with data: {signals_with_data} / {len(signals_df)} ({signals_with_data/len(signals_df):.1%})")
    print(f"Total posts analyzed: {total_posts}")
    print(f"Average posts per signal: {total_posts/len(signals_df):.1f}")
    print("")
    
    # Ticker breakdown
    print("TICKER BREAKDOWN")
    print("-" * 30)
    for ticker in tickers:
        ticker_data = signals_df[signals_df['ticker'] == ticker]
        ticker_with_data = len(ticker_data[ticker_data['metadata'].apply(lambda x: x.get('posts_analyzed', 0) > 0)])
        avg_sentiment = ticker_data['value'].mean()
        avg_confidence = ticker_data['confidence'].mean()
        
        print(f"{ticker}: {ticker_with_data}/{len(ticker_data)} signals with data, "
              f"avg sentiment: {avg_sentiment:.3f}, avg confidence: {avg_confidence:.3f}")
    print("")
    
    # Export for backtesting
    if args.output:
        output_file = args.output
    else:
        output_file = f"data/simple_sentiment_{start_date}_{end_date}.csv"
    
    success = export_signals_for_backtesting(signals_df, output_file)
    
    if success:
        print(f"SUCCESS: Signals exported to: {output_file}")
        print("")
        print("EXPORT FORMAT")
        print("-" * 30)
        print("Columns: asof_date, ticker, signal_name, value, confidence, posts_analyzed, calculation_method, search_terms")
        print("Format: CSV with point-in-time sentiment scores")
        print("Ready for: External backtesting software integration")
    else:
        print("ERROR: Export failed")
    
    return signals_df


def main():
    """Main entry point for simple sentiment analysis."""
    parser = argparse.ArgumentParser(
        description='Simple Reddit Sentiment Analysis for Backtesting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate sentiment for last 30 days (default)
  python simple_sentiment_main.py generate
  
  # Generate sentiment for specific date range
  python simple_sentiment_main.py generate --start-date 2024-01-01 --end-date 2024-01-31
  
  # Generate sentiment for specific tickers
  python simple_sentiment_main.py generate --tickers "EA,MSFT,SONY"
  
  # Generate sentiment with custom lookback period
  python simple_sentiment_main.py generate --lookback-days 60
  
  # Generate sentiment with custom output file
  python simple_sentiment_main.py generate --output "my_signals.csv"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Generate command
    generate_parser = subparsers.add_parser('generate', help='Generate simple sentiment signals')
    generate_parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    generate_parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    generate_parser.add_argument('--tickers', type=str, help='Comma-separated list of tickers')
    generate_parser.add_argument('--lookback-days', type=int, default=30, 
                               help='Number of days to look back for sentiment calculation')
    generate_parser.add_argument('--output', type=str, help='Output CSV file path')
    
    args = parser.parse_args()
    
    if args.command == 'generate':
        generate_sentiment_signals(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
