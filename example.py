#!/usr/bin/env python3
"""
Example script demonstrating Reddit sentiment signal processing.

This script shows how to use the sentiment-reddit signal processor
with different configurations and parameters.
"""

import sys
from pathlib import Path
from datetime import date, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.calculate_signals import calculate_signals


def example_basic_usage():
    """Demonstrate basic signal calculation."""
    print("=" * 60)
    print("EXAMPLE 1: Basic Signal Calculation")
    print("=" * 60)
    
    result = calculate_signals()
    
    print(f"Generated {result['signals_count']} signals")
    print(f"Tickers: {', '.join(result['parameters']['tickers'])}")
    print(f"Date range: {result['parameters']['start_date']} to {result['parameters']['end_date']}")
    print(f"Average sentiment: {result['summary_stats']['average_sentiment']:.3f}")
    print()


def example_custom_parameters():
    """Demonstrate custom parameter usage."""
    print("=" * 60)
    print("EXAMPLE 2: Custom Parameters")
    print("=" * 60)
    
    # Custom parameters
    tickers = ['AAPL', 'MSFT', 'TSLA']
    start_date = date.today() - timedelta(days=7)
    end_date = date.today()
    subreddits = ['stocks', 'investing', 'SecurityAnalysis']
    
    result = calculate_signals(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        subreddits=subreddits
    )
    
    print(f"Generated {result['signals_count']} signals")
    print(f"Tickers: {', '.join(tickers)}")
    print(f"Subreddits: {', '.join(subreddits)}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Average sentiment: {result['summary_stats']['average_sentiment']:.3f}")
    print(f"Sentiment range: {result['summary_stats']['min_sentiment']:.3f} to {result['summary_stats']['max_sentiment']:.3f}")
    print()


def example_signal_analysis():
    """Demonstrate signal analysis and statistics."""
    print("=" * 60)
    print("EXAMPLE 3: Signal Analysis")
    print("=" * 60)
    
    result = calculate_signals(
        tickers=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN'],
        start_date=date.today() - timedelta(days=14),
        end_date=date.today()
    )
    
    signals_df = result['signals_df']
    
    print("Signal Statistics by Ticker:")
    print("-" * 40)
    
    for ticker in result['parameters']['tickers']:
        ticker_data = signals_df[signals_df['ticker'] == ticker]
        if not ticker_data.empty:
            avg_sentiment = ticker_data['value'].mean()
            std_sentiment = ticker_data['value'].std()
            min_sentiment = ticker_data['value'].min()
            max_sentiment = ticker_data['value'].max()
            
            print(f"{ticker:>6}: avg={avg_sentiment:6.3f}, std={std_sentiment:6.3f}, "
                  f"range=[{min_sentiment:6.3f}, {max_sentiment:6.3f}]")
    
    print()
    print("Recent Signals (last 5 days):")
    print("-" * 40)
    
    recent_signals = signals_df.sort_values('asof_date').tail(10)
    for _, row in recent_signals.iterrows():
        print(f"{row['asof_date']}: {row['ticker']:>6} = {row['value']:6.3f}")
    
    print()


def main():
    """Run all examples."""
    print("REDDIT SENTIMENT SIGNAL PROCESSOR - EXAMPLES")
    print("=" * 60)
    print()
    
    try:
        example_basic_usage()
        example_custom_parameters()
        example_signal_analysis()
        
        print("=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        print()
        print("Next steps for development:")
        print("1. Implement Reddit API integration")
        print("2. Add real sentiment analysis")
        print("3. Create database storage")
        print("4. Add configuration management")
        print("5. Implement error handling and logging")
        
    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
