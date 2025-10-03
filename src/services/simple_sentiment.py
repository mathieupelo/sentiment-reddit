"""
Simple Reddit sentiment analysis using only ticker and company name.

This module provides a straightforward sentiment analysis that searches for
ticker symbols and company names without complex keyword mapping.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
import time
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.calculate_signals import reddit_client, analyze_sentiment

logger = logging.getLogger(__name__)


class SimpleSentimentAnalyzer:
    """Simple sentiment analyzer using only ticker and company name."""
    
    def __init__(self):
        """Initialize the simple sentiment analyzer."""
        # Simple company name mapping
        self.company_names = {
            'EA': 'electronic arts',
            'MSFT': 'microsoft',
            'SONY': 'sony',
            'TTWO': 'take-two',
            'NTES': 'netease',
            'RBLX': 'roblox',
            'WBD': 'warner bros',
            'NCBDY': 'cd projekt',
            'GDEV': 'gaijin',
            'OTGLF': 'outlook',
            'SNAL': 'snail',
            'GRVY': 'gravity'
        }
    
    def get_simple_sentiment(self, ticker: str, target_date: date, 
                           lookback_days: int = 30) -> Dict[str, Any]:
        """
        Get sentiment using only ticker and company name.
        
        Args:
            ticker: Stock ticker symbol
            target_date: Date for sentiment calculation
            lookback_days: Number of days to look back for sentiment
            
        Returns:
            Dictionary with sentiment analysis
        """
        logger.info(f"Calculating simple sentiment for {ticker} on {target_date}")
        
        # Get search terms (ticker + company name)
        search_terms = [ticker.lower()]
        if ticker in self.company_names:
            search_terms.append(self.company_names[ticker])
        
        # Search for posts
        all_posts = []
        
        try:
            for term in search_terms:
                posts = self._search_posts(term, target_date, lookback_days)
                all_posts.extend(posts)
                
                # Rate limiting
                time.sleep(0.5)
        
        except Exception as e:
            logger.error(f"Error searching for {ticker}: {e}")
            return {
                'ticker': ticker,
                'date': target_date,
                'sentiment_score': 0.0,
                'confidence': 0.0,
                'posts_analyzed': 0,
                'calculation_method': 'error',
                'search_terms': search_terms
            }
        
        # Calculate sentiment from posts
        if not all_posts:
            return {
                'ticker': ticker,
                'date': target_date,
                'sentiment_score': 0.0,
                'confidence': 0.0,
                'posts_analyzed': 0,
                'calculation_method': 'no_data',
                'search_terms': search_terms
            }
        
        # Analyze sentiment for each post
        sentiments = []
        weights = []
        
        for post in all_posts:
            # Combine title and content
            text = f"{post['title']} {post['content']}"
            sentiment = analyze_sentiment(text)
            sentiments.append(sentiment)
            
            # Weight by engagement and recency
            engagement_weight = post['score'] + (post['num_comments'] * 2)
            recency_weight = 1.0 / (1.0 + (target_date - post['date']).days)
            weight = engagement_weight * recency_weight
            weights.append(weight)
        
        # Calculate weighted average sentiment
        total_weight = sum(weights)
        if total_weight > 0:
            normalized_weights = [w / total_weight for w in weights]
            weighted_sentiment = sum(s * w for s, w in zip(sentiments, normalized_weights))
        else:
            weighted_sentiment = sum(sentiments) / len(sentiments)
        
        # Calculate confidence based on data quality
        confidence = min(len(all_posts) / 20.0, 1.0)  # Max confidence at 20+ posts
        
        return {
            'ticker': ticker,
            'date': target_date,
            'sentiment_score': weighted_sentiment,
            'confidence': confidence,
            'posts_analyzed': len(all_posts),
            'calculation_method': 'simple_ticker_company',
            'search_terms': search_terms
        }
    
    def _search_posts(self, search_term: str, target_date: date, 
                     lookback_days: int) -> List[Dict]:
        """Search for posts using a search term within the lookback period."""
        if not reddit_client.is_available():
            return []
        
        posts = []
        
        try:
            # Search across all of Reddit
            search_results = reddit_client.reddit.subreddit('all').search(
                search_term, limit=50, time_filter='month'
            )
            
            for post in search_results:
                post_date = datetime.fromtimestamp(post.created_utc).date()
                
                # Check if post is within lookback period
                if post_date >= (target_date - timedelta(days=lookback_days)) and post_date <= target_date:
                    posts.append({
                        'id': post.id,
                        'title': post.title,
                        'content': post.selftext if hasattr(post, 'selftext') else '',
                        'subreddit': str(post.subreddit),
                        'score': post.score,
                        'num_comments': post.num_comments,
                        'created_utc': post.created_utc,
                        'date': post_date
                    })
        
        except Exception as e:
            logger.warning(f"Error searching for '{search_term}': {e}")
        
        return posts
    
    def calculate_point_in_time_sentiment(self, tickers: List[str], start_date: date, 
                                        end_date: date, lookback_days: int = 30) -> pd.DataFrame:
        """
        Calculate point-in-time sentiment for multiple tickers over a date range.
        
        Args:
            tickers: List of ticker symbols
            start_date: Start date for calculation
            end_date: End date for calculation
            lookback_days: Number of days to look back for sentiment
            
        Returns:
            DataFrame with point-in-time sentiment signals
        """
        logger.info(f"Calculating simple sentiment for {len(tickers)} tickers from {start_date} to {end_date}")
        
        signals_data = []
        
        current_date = start_date
        while current_date <= end_date:
            logger.info(f"Processing {current_date}")
            
            for ticker in tickers:
                # Calculate sentiment using only ticker and company name
                sentiment_result = self.get_simple_sentiment(ticker, current_date, lookback_days)
                
                signals_data.append({
                    'asof_date': current_date,
                    'ticker': ticker,
                    'signal_name': 'SIMPLE_SENTIMENT_RDDT',
                    'value': sentiment_result['sentiment_score'],
                    'confidence': sentiment_result['confidence'],
                    'metadata': {
                        'calculation_method': sentiment_result['calculation_method'],
                        'data_source': 'simple_reddit_api',
                        'posts_analyzed': sentiment_result['posts_analyzed'],
                        'search_terms': sentiment_result['search_terms'],
                        'lookback_days': lookback_days,
                        'point_in_time': True
                    }
                })
            
            current_date += timedelta(days=1)
        
        return pd.DataFrame(signals_data)


# Global instance
simple_sentiment_analyzer = SimpleSentimentAnalyzer()


def calculate_simple_sentiment(tickers: List[str], start_date: date, end_date: date,
                             lookback_days: int = 30) -> pd.DataFrame:
    """
    Calculate simple point-in-time sentiment for multiple tickers.
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date for calculation
        end_date: End date for calculation
        lookback_days: Number of days to look back for sentiment
        
    Returns:
        DataFrame with simple sentiment signals
    """
    return simple_sentiment_analyzer.calculate_point_in_time_sentiment(
        tickers, start_date, end_date, lookback_days
    )


def export_signals_for_backtesting(signals_df: pd.DataFrame, output_file: str) -> bool:
    """
    Export sentiment signals in a format suitable for external backtesting.
    
    Args:
        signals_df: DataFrame with sentiment signals
        output_file: Output file path
        
    Returns:
        True if export successful, False otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export to CSV with clean format for backtesting
        export_df = signals_df.copy()
        
        # Flatten metadata for easier analysis
        export_df['posts_analyzed'] = export_df['metadata'].apply(
            lambda x: x.get('posts_analyzed', 0)
        )
        export_df['calculation_method'] = export_df['metadata'].apply(
            lambda x: x.get('calculation_method', 'unknown')
        )
        export_df['search_terms'] = export_df['metadata'].apply(
            lambda x: ', '.join(x.get('search_terms', []))
        )
        
        # Select relevant columns for backtesting
        backtest_columns = [
            'asof_date', 'ticker', 'signal_name', 'value', 'confidence',
            'posts_analyzed', 'calculation_method', 'search_terms'
        ]
        
        export_df = export_df[backtest_columns]
        
        # Export to CSV
        export_df.to_csv(output_file, index=False)
        
        logger.info(f"Exported {len(export_df)} signals to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting signals: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    from datetime import date, timedelta
    
    # Calculate simple sentiment for the last 7 days
    end_date = date.today()
    start_date = end_date - timedelta(days=7)
    tickers = ['EA', 'MSFT', 'RBLX']
    
    print("Calculating simple sentiment...")
    signals_df = calculate_simple_sentiment(tickers, start_date, end_date)
    
    print(f"Generated {len(signals_df)} simple sentiment signals")
    print(f"Average confidence: {signals_df['confidence'].mean():.3f}")
    print(f"Signals with data: {len(signals_df[signals_df['metadata'].apply(lambda x: x.get('posts_analyzed', 0) > 0)])}")
    
    # Export for backtesting
    export_signals_for_backtesting(signals_df, "data/simple_sentiment_signals.csv")
    print("Signals exported for backtesting")
