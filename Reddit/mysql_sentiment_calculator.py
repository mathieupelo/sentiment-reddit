"""
MySQL Sentiment Calculator
Calculates sentiment signals from Reddit posts stored in MySQL database.
Perfect for backtesting with historical data.
"""

import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
import pandas as pd
import numpy as np
from textblob import TextBlob
import re
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.calculate_signals import (
    analyze_sentiment, 
    aggregate_sentiment_scores, 
    _clean_text, 
    _calculate_gaming_keyword_sentiment,
    finbert_analyzer
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MySQLSentimentCalculator:
    """Calculates sentiment signals from MySQL database."""
    
    def __init__(self, host='localhost', user='root', password='', database='reddit_sentiment'):
        """Initialize MySQL connection."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        
    def connect(self):
        """Connect to MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                logger.info(f"Connected to MySQL database: {self.database}")
                return True
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MySQL database."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")
    
    def get_posts_for_date_range(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get posts for a specific ticker and date range."""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            query = """
                SELECT reddit_id, title, content, author, subreddit, created_utc, 
                       created_date, score, num_comments, url, is_self, ticker, keyword_matched
                FROM reddit_posts 
                WHERE ticker = %s 
                AND created_date BETWEEN %s AND %s
                ORDER BY created_date, created_utc
            """
            
            cursor.execute(query, (ticker, start_date, end_date))
            posts = cursor.fetchall()
            
            if not posts:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(posts)
            
            # Convert Unix timestamp to datetime
            df['created_utc'] = df['created_utc'].astype(float)
            
            logger.info(f"Retrieved {len(df)} posts for {ticker} from {start_date} to {end_date}")
            return df
            
        except Error as e:
            logger.error(f"Error retrieving posts for {ticker}: {e}")
            return pd.DataFrame()
    
    def calculate_sentiment_for_posts(self, posts_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate sentiment scores for posts."""
        if posts_df.empty:
            return posts_df
        
        # Calculate sentiment for each post
        posts_df = posts_df.copy()
        
        # Combine title and content for sentiment analysis
        posts_df['full_text'] = posts_df['title'].fillna('') + ' ' + posts_df['content'].fillna('')
        
        # Calculate sentiment scores
        posts_df['sentiment_score'] = posts_df['full_text'].apply(analyze_sentiment)
        
        # Fill NaN values with 0
        posts_df['sentiment_score'] = posts_df['sentiment_score'].fillna(0.0)
        
        logger.info(f"Calculated sentiment for {len(posts_df)} posts")
        return posts_df
    
    def calculate_daily_sentiment(self, ticker: str, target_date: date, lookback_days: int = 30) -> Dict:
        """
        Calculate sentiment for a specific ticker and date using lookback window.
        
        Args:
            ticker: Stock ticker symbol
            target_date: Date to calculate sentiment for
            lookback_days: Number of days to look back for posts
            
        Returns:
            Dictionary with sentiment data
        """
        # Calculate lookback period for THIS specific date
        lookback_start = target_date - timedelta(days=lookback_days)
        
        # Get posts for the lookback period (30 days before target_date)
        posts_df = self.get_posts_for_date_range(ticker, lookback_start, target_date)
        
        if posts_df.empty:
            return {
                'asof_date': target_date,
                'ticker': ticker,
                'sentiment_score': 0.0,
                'posts_analyzed': 0,
                'confidence': 0.0,
                'calculation_method': 'fallback_no_data'
            }
        
        # Calculate sentiment scores
        posts_df = self.calculate_sentiment_for_posts(posts_df)
        
        # Aggregate sentiment scores
        aggregated_score = aggregate_sentiment_scores(posts_df, ticker, target_date)
        
        # Calculate confidence based on number of posts
        posts_analyzed = len(posts_df)
        confidence = min(posts_analyzed / 50.0, 1.0) if posts_analyzed > 0 else 0.0
        
        # Determine which method was actually used
        calculation_method = 'finbert_with_gaming_keywords' if finbert_analyzer.available else 'textblob_with_gaming_keywords'
        
        return {
            'asof_date': target_date,
            'ticker': ticker,
            'sentiment_score': aggregated_score,
            'posts_analyzed': posts_analyzed,
            'confidence': confidence,
            'calculation_method': calculation_method
        }
    
    def calculate_signals_for_period(self, tickers: List[str], start_date: date, end_date: date, 
                                   lookback_days: int = 30) -> pd.DataFrame:
        """
        Calculate sentiment signals for multiple tickers over a date range.
        
        Args:
            tickers: List of stock tickers
            start_date: Start date for signals
            end_date: End date for signals
            lookback_days: Lookback window for posts
            
        Returns:
            DataFrame with sentiment signals
        """
        signals_data = []
        current_date = start_date
        total_dates = (end_date - start_date).days + 1
        
        logger.info(f"Calculating sentiment signals for {len(tickers)} tickers")
        logger.info(f"Date range: {start_date} to {end_date} ({total_dates} days)")
        logger.info(f"Lookback window: {lookback_days} days")
        logger.info("=" * 60)
        
        while current_date <= end_date:
            date_progress = (current_date - start_date).days + 1
            
            if date_progress % 50 == 0 or date_progress == total_dates:
                logger.info(f"[{date_progress}/{total_dates}] Processing {current_date}...")
            
            for ticker in tickers:
                # Calculate sentiment for this ticker and date
                sentiment_data = self.calculate_daily_sentiment(ticker, current_date, lookback_days)
                
                # Add to signals data
                signals_data.append({
                    'asof_date': current_date,
                    'ticker': ticker,
                    'signal_name': 'SENTIMENT_RDDT',
                    'value': sentiment_data['sentiment_score'],
                    'confidence': sentiment_data['confidence'],
                    'posts_analyzed': sentiment_data['posts_analyzed'],
                    'calculation_method': sentiment_data['calculation_method'],
                    'metadata': {
                        'data_source': 'mysql_database',
                        'lookback_days': lookback_days,
                        'has_data': sentiment_data['posts_analyzed'] > 0
                    }
                })
            
            current_date += timedelta(days=1)
        
        # Convert to DataFrame
        signals_df = pd.DataFrame(signals_data)
        
        logger.info(f"\nGenerated {len(signals_df)} sentiment signals")
        logger.info(f"Signals with data: {len(signals_df[signals_df['posts_analyzed'] > 0])}")
        logger.info(f"Fallback signals: {len(signals_df[signals_df['posts_analyzed'] == 0])}")
        
        return signals_df
    
    def get_data_summary(self) -> Dict:
        """Get summary of available data in the database."""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Total posts
            cursor.execute("SELECT COUNT(*) as total FROM reddit_posts")
            total_posts = cursor.fetchone()['total']
            
            # Posts by ticker
            cursor.execute("""
                SELECT ticker, COUNT(*) as count 
                FROM reddit_posts 
                GROUP BY ticker 
                ORDER BY count DESC
            """)
            by_ticker = cursor.fetchall()
            
            # Date range
            cursor.execute("""
                SELECT MIN(created_date) as min_date, MAX(created_date) as max_date 
                FROM reddit_posts
            """)
            date_range = cursor.fetchone()
            
            # Posts by date (last 30 days)
            cursor.execute("""
                SELECT created_date, COUNT(*) as count 
                FROM reddit_posts 
                WHERE created_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                GROUP BY created_date 
                ORDER BY created_date DESC
                LIMIT 10
            """)
            recent_posts = cursor.fetchall()
            
            return {
                'total_posts': total_posts,
                'by_ticker': by_ticker,
                'date_range': date_range,
                'recent_posts': recent_posts
            }
            
        except Error as e:
            logger.error(f"Error getting data summary: {e}")
            return {}


def main():
    """Main function to run the sentiment calculator."""
    print("=" * 60)
    print("MYSQL SENTIMENT CALCULATOR")
    print("=" * 60)
    print()
    
    # Configuration
    tickers = ['EA', 'TTWO', 'NTES', 'RBLX', 'MSFT', 'SONY', 
               'WBD', 'NCBDY', 'GDEV', 'OTGLF', 'SNAL', 'GRVY',
               'SQNXF', 'KSFTF', 'KNMCY', 'NEXOY']
    
    print(f"Tickers: {', '.join(tickers)}")
    print()
    
    # Get MySQL credentials from environment variables
    print("MySQL Configuration:")
    host = os.getenv('MYSQL_HOST', 'localhost')
    user = os.getenv('MYSQL_USER', 'root')
    password = os.getenv('MYSQL_PASSWORD', '')
    database = os.getenv('MYSQL_DATABASE', 'reddit_sentiment')
    
    print(f"Host: {host}")
    print(f"User: {user}")
    print(f"Database: {database}")
    print()
    
    # Create calculator
    calculator = MySQLSentimentCalculator(host=host, user=user, password=password, database=database)
    
    # Connect to database
    if not calculator.connect():
        print("Failed to connect to MySQL. Exiting.")
        return
    
    print("Connected to MySQL successfully!")
    print()
    
    # Show data summary
    print("Database Summary:")
    summary = calculator.get_data_summary()
    if summary:
        print(f"Total posts: {summary['total_posts']}")
        print(f"Date range: {summary['date_range']['min_date']} to {summary['date_range']['max_date']}")
        print("\nPosts by ticker:")
        for row in summary['by_ticker']:
            print(f"  {row['ticker']:6}: {row['count']:5} posts")
    print()
    
    # Get date range
    print("Date Range Configuration:")
    days_back = input("Days to look back (default: 700): ").strip()
    days_back = int(days_back) if days_back else 700
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    print(f"Calculating signals from {start_date} to {end_date} ({days_back} days)")
    print()
    
    # Confirm before proceeding
    response = input("Proceed with sentiment calculation? (y/N): ").strip().lower()
    if response != 'y':
        print("Calculation cancelled.")
        calculator.disconnect()
        return
    
    # Calculate signals
    signals_df = calculator.calculate_signals_for_period(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        lookback_days=30
    )
    
    # Save to CSV
    output_file = f"data/mysql_sentiment_{start_date}_{end_date}.csv"
    from pathlib import Path
    Path("data").mkdir(exist_ok=True)
    
    try:
        signals_df.to_csv(output_file, index=False)
        print(f"\nSUCCESS: Signals saved to {output_file}")
        
        # Show summary
        print(f"\nSummary:")
        print(f"Total signals: {len(signals_df)}")
        print(f"Signals with data: {len(signals_df[signals_df['posts_analyzed'] > 0])}")
        print(f"Average sentiment: {signals_df['value'].mean():.4f}")
        print(f"Average confidence: {signals_df['confidence'].mean():.4f}")
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        print("ERROR: Failed to save results")
    
    # Disconnect
    calculator.disconnect()
    print("\nDone!")


if __name__ == "__main__":
    main()
