#!/usr/bin/env python3
"""
Unified Reddit Sentiment Analysis - Fixed version with hardcoded credentials.
"""

import logging
import os
import sys
import argparse
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.calculate_signals import finbert_analyzer, calculate_signals_optimized
from mysql_sentiment_calculator import MySQLSentimentCalculator
from reddit_mysql_collector import RedditMySQLCollector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedSentimentAnalysis:
    """Unified sentiment analysis with MySQL data reuse."""
    
    def __init__(self, signal_days: int = 14, lookback_days: int = 30):
        """
        Initialize with hardcoded MySQL configuration.
        
        Args:
            signal_days: Number of days to generate signals for (default: 14)
            lookback_days: Number of days to look back for each signal (default: 30)
        """
        self.mysql_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '3421',
            'database': 'reddit_sentiment'
        }
        
        self.calculator = None
        self.collector = None
        self.connected = False
        
        # Signal generation parameters
        self.signal_days = signal_days
        self.lookback_days = lookback_days
        # Collection days = signal days + lookback (to have enough data for all signals)
        self.collection_days = signal_days + lookback_days
        
        logger.info(f"Configuration: {signal_days} signal days, {lookback_days} lookback, {self.collection_days} collection days")
        
        # All 16 tickers
        self.tickers = [
            'EA', 'TTWO', 'NTES', 'RBLX', 'MSFT', 'SONY', 
            'WBD', 'NCBDY', 'GDEV', 'OTGLF', 'SNAL', 'GRVY',
            'SQNXF', 'KSFTF', 'KNMCY', 'NEXOY'
        ]
        
        # Subreddits for data collection
        self.subreddits = [
            'gaming', 'pcgaming', 'xbox', 'playstation', 'nintendo', 'games', 'truegaming',
            'Steam', 'GamingLeaksAndRumours', 'GameDeals', 'patientgamers', 'ShouldIbuythisgame',
            'PS5', 'XboxSeriesX', 'NintendoSwitch', 'SteamDeck', 'GameStop',
            'investing', 'stocks', 'wallstreetbets', 'technology'
        ]
    
    def connect(self) -> bool:
        """Connect to MySQL database."""
        try:
            self.calculator = MySQLSentimentCalculator(**self.mysql_config)
            self.collector = RedditMySQLCollector(**self.mysql_config)
            
            if self.calculator.connect() and self.collector.connect():
                self.connected = True
                logger.info("✅ Connected to MySQL database successfully")
                return True
            else:
                logger.error("❌ Failed to connect to MySQL database")
                return False
        except Exception as e:
            logger.error(f"❌ Error connecting to MySQL: {e}")
            logger.warning("⚠️ Will use direct Reddit API mode (no MySQL storage)")
            return False
    
    def disconnect(self):
        """Disconnect from MySQL database."""
        if self.calculator:
            self.calculator.disconnect()
        if self.collector:
            self.collector.disconnect()
        self.connected = False
        logger.info("Disconnected from MySQL database")
    
    def check_data_coverage(self, start_date: date, end_date: date) -> Dict[str, bool]:
        """Check which tickers need data collection for the given date range."""
        if not self.connected:
            logger.error("Not connected to MySQL database")
            return {}
        
        logger.info(f"Checking data coverage for {len(self.tickers)} tickers from {start_date} to {end_date}")
        
        # Calculate proportional minimum posts based on time window
        # Base: 1000 posts for 700 days, scaled proportionally with minimum of 50
        days_in_window = (end_date - start_date).days + 1
        min_posts_required = max(50, int((days_in_window / 700.0) * 1000))
        logger.info(f"Minimum posts required for {days_in_window}-day window: {min_posts_required}")
        
        tickers_needing_data = {}
        
        for ticker in self.tickers:
            try:
                posts_df = self.calculator.get_posts_for_date_range(ticker, start_date, end_date)
                
                if posts_df.empty:
                    tickers_needing_data[ticker] = True
                    logger.info(f"{ticker}: No data found - needs collection")
                else:
                    total_posts = len(posts_df)
                    if total_posts < min_posts_required:
                        tickers_needing_data[ticker] = True
                        logger.info(f"{ticker}: Only {total_posts} posts - needs more data (min: {min_posts_required})")
                    else:
                        tickers_needing_data[ticker] = False
                        logger.info(f"{ticker}: {total_posts} posts - sufficient data")
                        
            except Exception as e:
                logger.error(f"Error checking data for {ticker}: {e}")
                tickers_needing_data[ticker] = True
        
        return tickers_needing_data
    
    def collect_missing_data(self, start_date: date, end_date: date) -> bool:
        """Collect missing Reddit data for tickers that need it."""
        logger.info("Checking for missing data...")
        
        tickers_needing_data = self.check_data_coverage(start_date, end_date)
        tickers_to_collect = [ticker for ticker, needs_data in tickers_needing_data.items() if needs_data]
        
        if not tickers_to_collect:
            logger.info("✅ All tickers have sufficient data coverage")
            return True
        
        logger.info(f"📥 Collecting data for {len(tickers_to_collect)} tickers: {tickers_to_collect}")
        
        try:
            self.collector.collect_posts(
                tickers=tickers_to_collect,
                subreddits=self.subreddits,
                limit_per_search=100
            )
            
            logger.info("✅ Data collection completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during data collection: {e}")
            return False
    
    def generate_sentiment_signals_mysql(self) -> pd.DataFrame:
        """Generate sentiment signals using MySQL data."""
        end_date = date.today()
        start_date = end_date - timedelta(days=self.signal_days - 1)  # -1 because we include end_date
        
        logger.info(f"📊 Generating sentiment signals for {len(self.tickers)} tickers using MySQL")
        logger.info(f"📅 Signal date range: {start_date} to {end_date} ({self.signal_days} days)")
        logger.info(f"🔄 Each signal uses last {self.lookback_days} days of posts (no look-ahead bias)")
        
        try:
            signals_df = self.calculator.calculate_signals_for_period(
                tickers=self.tickers,
                start_date=start_date,
                end_date=end_date,
                lookback_days=self.lookback_days
            )
            
            logger.info(f"✅ Generated {len(signals_df)} sentiment signals")
            return signals_df
            
        except Exception as e:
            logger.error(f"❌ Error generating sentiment signals: {e}")
            return pd.DataFrame()
    
    def generate_sentiment_signals_direct(self) -> pd.DataFrame:
        """Generate sentiment signals using direct Reddit API calls."""
        end_date = date.today()
        start_date = end_date - timedelta(days=self.signal_days - 1)  # -1 because we include end_date
        
        logger.info(f"📊 Generating sentiment signals for {len(self.tickers)} tickers using direct Reddit API")
        logger.info(f"📅 Signal date range: {start_date} to {end_date} ({self.signal_days} days)")
        logger.info(f"🔄 Each signal uses last {self.lookback_days} days of posts (no look-ahead bias)")
        logger.warning("⚠️ Note: This will take longer as it fetches data from Reddit API directly")
        
        try:
            result = calculate_signals_optimized(
                tickers=self.tickers,
                start_date=start_date,
                end_date=end_date,
                use_cache=False
            )
            
            signals_df = result['signals_df']
            logger.info(f"✅ Generated {len(signals_df)} sentiment signals")
            return signals_df
            
        except Exception as e:
            logger.error(f"❌ Error generating sentiment signals: {e}")
            return pd.DataFrame()
    
    def generate_sentiment_signals(self) -> pd.DataFrame:
        """Generate sentiment signals using MySQL if available, otherwise direct API."""
        if self.connected:
            return self.generate_sentiment_signals_mysql()
        else:
            return self.generate_sentiment_signals_direct()
    
    def export_to_csv(self, signals_df: pd.DataFrame) -> bool:
        """Export sentiment signals to CSV file for backtesting."""
        if signals_df.empty:
            logger.error("No signals to export")
            return False
        
        try:
            output_dir = Path("data")
            output_dir.mkdir(exist_ok=True)
            
            start_date = signals_df['asof_date'].min()
            end_date = signals_df['asof_date'].max()
            output_file = f"data/unified_sentiment_{start_date}_{end_date}.csv"
            
            export_df = signals_df.copy()
            
            # Check if posts_analyzed and calculation_method are already columns
            # (they are when using MySQL, but not when using direct API)
            if 'posts_analyzed' not in export_df.columns:
                if 'metadata' in export_df.columns:
                    export_df['posts_analyzed'] = export_df['metadata'].apply(
                        lambda x: x.get('posts_analyzed', 0) if isinstance(x, dict) else 0
                    )
                else:
                    export_df['posts_analyzed'] = 0
            
            if 'calculation_method' not in export_df.columns:
                if 'metadata' in export_df.columns:
                    export_df['calculation_method'] = export_df['metadata'].apply(
                        lambda x: x.get('calculation_method', 'fallback_no_data') if isinstance(x, dict) else 'fallback_no_data'
                    )
                else:
                    export_df['calculation_method'] = 'fallback_no_data'
            
            # Set sentiment value to NaN for low confidence signals (< 0.1)
            # This filters out signals with fewer than 5 posts while keeping metadata visible
            import numpy as np
            low_confidence_mask = export_df['confidence'] < 0.1
            export_df.loc[low_confidence_mask, 'value'] = np.nan
            export_df.loc[low_confidence_mask, 'calculation_method'] = 'insufficient_confidence'
            
            logger.info(f"Set {low_confidence_mask.sum()} low-confidence signals (< 0.1) to NaN")
            
            from utils.calculate_signals import TICKER_GAME_MAPPING
            export_df['search_terms'] = export_df['ticker'].apply(
                lambda x: f"{x.lower()}, {', '.join([name.lower() for name in TICKER_GAME_MAPPING.get(x, [x])])}"
            )
            
            export_columns = [
                'asof_date', 'ticker', 'signal_name', 'value', 'confidence',
                'posts_analyzed', 'calculation_method', 'search_terms'
            ]
            
            for col in export_columns:
                if col not in export_df.columns:
                    export_df[col] = ''
            
            export_df = export_df[export_columns]
            export_df.to_csv(output_file, index=False)
            
            logger.info(f"✅ Successfully exported {len(export_df)} signals to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error exporting to CSV: {e}")
            return False
    
    def run_analysis(self) -> bool:
        """Run the complete unified sentiment analysis."""
        logger.info("=" * 80)
        logger.info("🚀 UNIFIED REDDIT SENTIMENT ANALYSIS")
        logger.info("=" * 80)
        logger.info(f"📈 Tickers: {len(self.tickers)} gaming companies")
        logger.info(f"📅 Signal generation: Last {self.signal_days} days")
        logger.info(f"📊 Data collection: Last {self.collection_days} days (includes {self.lookback_days}-day lookback)")
        logger.info(f"🔄 Lookback window: {self.lookback_days} days (no look-ahead bias)")
        logger.info(f"🤖 FinBERT available: {finbert_analyzer.available}")
        logger.info("=" * 80)
        
        try:
            logger.info("\n🔌 Step 1: Connecting to MySQL database...")
            mysql_available = self.connect()
            
            if mysql_available:
                # Ask user if they want to perform data collection
                print("\n" + "=" * 60)
                response = input("Perform data collection? (y/N): ").strip().lower()
                print("=" * 60)
                
                if response == 'y':
                    end_date = date.today()
                    # Collection window includes the lookback period
                    start_date = end_date - timedelta(days=self.collection_days - 1)
                    
                    logger.info("\n📊 Step 2: Checking and collecting missing data...")
                    logger.info(f"Collection window: {start_date} to {end_date}")
                    if not self.collect_missing_data(start_date, end_date):
                        logger.error("Failed to collect missing data")
                        return False
                else:
                    logger.info("\n📊 Step 2: Skipping data collection (user declined)")
                
                logger.info("\n🧠 Step 3: Generating sentiment signals...")
            else:
                logger.info("\n📊 Step 2: Skipping data collection (MySQL not available)")
                logger.info("\n🧠 Step 3: Generating sentiment signals...")
            
            signals_df = self.generate_sentiment_signals()
            
            if signals_df.empty:
                logger.error("No sentiment signals generated")
                return False
            
            logger.info("\n💾 Step 4: Exporting results to CSV...")
            if not self.export_to_csv(signals_df):
                logger.error("Failed to export signals")
                return False
            
            logger.info("\n" + "=" * 80)
            logger.info("🎉 ANALYSIS COMPLETE - SUMMARY")
            logger.info("=" * 80)
            
            signals_with_data = len(signals_df[signals_df['posts_analyzed'] > 0])
            total_posts = signals_df['posts_analyzed'].sum()
            
            logger.info(f"📊 Total signals generated: {len(signals_df)}")
            logger.info(f"✅ Signals with data: {signals_with_data} / {len(signals_df)} ({signals_with_data/len(signals_df):.1%})")
            logger.info(f"📝 Total posts analyzed: {total_posts}")
            logger.info(f"📈 Average sentiment: {signals_df['value'].mean():.3f}")
            logger.info(f"🎯 Average confidence: {signals_df['confidence'].mean():.3f}")
            logger.info(f"🤖 FinBERT available: {finbert_analyzer.available}")
            
            logger.info("\n📋 Ticker breakdown:")
            for ticker in self.tickers:
                ticker_data = signals_df[signals_df['ticker'] == ticker]
                ticker_with_data = len(ticker_data[ticker_data['posts_analyzed'] > 0])
                avg_sentiment = ticker_data['value'].mean()
                avg_confidence = ticker_data['confidence'].mean()
                
                logger.info(f"  {ticker:6}: {ticker_with_data:4}/{len(ticker_data):4} signals with data, "
                           f"avg sentiment: {avg_sentiment:7.3f}, avg confidence: {avg_confidence:.3f}")
            
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during analysis: {e}")
            return False
        
        finally:
            self.disconnect()


def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Unified Reddit Sentiment Analysis',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--days',
        type=int,
        default=14,
        help='Number of days to generate signals for (default: 14)'
    )
    parser.add_argument(
        '--lookback',
        type=int,
        default=30,
        help='Lookback window in days for each signal (default: 30)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.days < 1:
        print("❌ Error: --days must be at least 1")
        return 1
    
    if args.lookback < 1:
        print("❌ Error: --lookback must be at least 1")
        return 1
    
    # Create analyzer with specified parameters
    analyzer = UnifiedSentimentAnalysis(
        signal_days=args.days,
        lookback_days=args.lookback
    )
    
    success = analyzer.run_analysis()
    
    if success:
        print("\n🎉 [SUCCESS] Unified sentiment analysis completed successfully!")
        return 0
    else:
        print("\n❌ [ERROR] Unified sentiment analysis failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
