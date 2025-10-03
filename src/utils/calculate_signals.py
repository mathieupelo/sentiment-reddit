"""
Signal calculation utilities for Reddit-based sentiment analysis.

This module provides the core signal calculation functionality for processing
Reddit data and generating sentiment signals. It follows the same pattern as
the sentiment-signals repository but is designed for Reddit data sources.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
from textblob import TextBlob
import re
import os
from dotenv import load_dotenv
import praw
import time

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Gaming-specific sentiment keywords for enhanced analysis
GAMING_POSITIVE_KEYWORDS = {
    'amazing', 'epic', 'awesome', 'fun', 'addictive', 'polished', 'smooth',
    'beautiful', 'stunning', 'incredible', 'fantastic', 'brilliant', 'perfect',
    'love', 'best', 'great', 'excellent', 'outstanding', 'masterpiece',
    'engaging', 'immersive', 'compelling', 'satisfying', 'rewarding'
}

GAMING_NEGATIVE_KEYWORDS = {
    'buggy', 'glitchy', 'broken', 'unplayable', 'laggy', 'pay-to-win',
    'terrible', 'awful', 'horrible', 'disappointing', 'frustrating', 'annoying',
    'boring', 'repetitive', 'generic', 'overpriced', 'scam', 'waste',
    'hate', 'worst', 'trash', 'garbage', 'uninstall', 'refund'
}

# Company-to-game mapping for better ticker identification
TICKER_GAME_MAPPING = {
    'EA': ['Electronic Arts', 'EA Games'],
    'TTWO': ['Take-Two Interactive', 'TTWO'],
    'NTES': ['NetEase'],
    'RBLX': ['Roblox'],
    'MSFT': ['Microsoft'],
    'SONY': ['Sony'],
    'WBD': ['Warner Bros'],
    'NCBDY': ['CD Projekt'],
    'GDEV': ['Gaijin'],
    'OTGLF': ['Outlook Games'],
    'SNAL': ['Snail Games'],
    'GRVY': ['Gravity Games']
}


class RedditAPIClient:
    """Reddit API client for fetching posts and comments."""
    
    def __init__(self):
        """Initialize Reddit API client with credentials from environment variables."""
        self.client_id = os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = os.getenv('REDDIT_USER_AGENT', 'sentiment_reddit_bot/1.0')
        
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Reddit API credentials not found. Please create a .env file with:\n"
                "REDDIT_CLIENT_ID=your_client_id\n"
                "REDDIT_CLIENT_SECRET=your_client_secret\n"
                "REDDIT_USER_AGENT=sentiment_reddit_bot/1.0"
            )
        
        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            # Test connection
            self.reddit.user.me()
            logger.info("Reddit API connection successful")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Reddit API: {e}")
    
    def is_available(self) -> bool:
        """Check if Reddit API is available."""
        return self.reddit is not None
    
    def search_posts(self, subreddits: List[str], keywords: List[str], 
                    limit: int = 100, time_filter: str = 'week', ticker: str = None) -> List[Dict]:
        """
        Search for posts in specified subreddits containing keywords.
        
        Args:
            subreddits: List of subreddit names to search
            keywords: List of keywords to search for
            limit: Maximum number of posts to return
            time_filter: Time filter ('hour', 'day', 'week', 'month', 'year', 'all')
            ticker: Ticker symbol to assign to posts (if None, uses first keyword)
            
        Returns:
            List of post dictionaries
        """
        if not self.is_available():
            return []
        
        # Use ticker if provided, otherwise use first keyword
        post_ticker = ticker if ticker else keywords[0] if keywords else 'unknown'
        
        posts = []
        try:
            for subreddit_name in subreddits:
                try:
                    subreddit = self.reddit.subreddit(subreddit_name)
                    
                    # Search for posts containing any of the keywords
                    for keyword in keywords:
                        search_query = f"{keyword}"
                        search_results = subreddit.search(
                            search_query, 
                            limit=limit // len(keywords), 
                            time_filter=time_filter,
                            sort='new'
                        )
                        
                        for post in search_results:
                            posts.append(self._extract_post_data(post, post_ticker))
                            
                        # Rate limiting
                        time.sleep(0.5)
                        
                except Exception as e:
                    logger.warning(f"Error searching subreddit {subreddit_name}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in Reddit search: {e}")
            
        return posts
    
    def _extract_post_data(self, post, ticker: str) -> Dict:
        """Extract relevant data from a Reddit post."""
        try:
            return {
                'id': post.id,
                'title': post.title,
                'content': post.selftext if hasattr(post, 'selftext') else '',
                'subreddit': str(post.subreddit),
                'ticker': ticker,
                'score': post.score,
                'num_comments': post.num_comments,
                'created_utc': post.created_utc,
                'author': str(post.author) if post.author else '[deleted]',
                'url': post.url,
                'is_self': post.is_self
            }
        except Exception as e:
            logger.warning(f"Error extracting post data: {e}")
            return {}


# Global Reddit API client instance
reddit_client = RedditAPIClient()


def calculate_signals(tickers: Optional[List[str]] = None, 
                     start_date: Optional[date] = None, 
                     end_date: Optional[date] = None,
                     subreddits: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Calculate Reddit-based sentiment signals for given tickers and date range.
    
    This function processes Reddit data and calculates sentiment scores using
    TextBlob and gaming-specific keyword analysis. GUARANTEES a score for every
    date and ticker combination.
    
    Args:
        tickers: List of stock tickers to calculate signals for
        start_date: Start date for signal calculation
        end_date: End date for signal calculation  
        subreddits: List of Reddit subreddits to analyze
        
    Returns:
        Dictionary containing signal calculation results and metadata
        
    Example:
        >>> result = calculate_signals(
        ...     tickers=['EA', 'MSFT'],
        ...     start_date=date(2024, 1, 1),
        ...     end_date=date(2024, 1, 31),
        ...     subreddits=['gaming', 'pcgaming']
        ... )
        >>> print(result['signals_count'])
        62  # 31 days * 2 tickers = 62 signals
    """
    
    # Set default values if not provided
    if tickers is None:
        tickers = ['EA', 'TTWO', 'NTES', 'RBLX', 'MSFT', 'SONY', 'WBD', 'NCBDY', 'GDEV', 'OTGLF', 'SNAL', 'GRVY']
    
    if start_date is None:
        start_date = date.today() - timedelta(days=90)  # Extended to 3 months
        
    if end_date is None:
        end_date = date.today()
        
    if subreddits is None:
        # Expanded gaming-focused subreddits for better coverage
        subreddits = [
            'gaming', 'pcgaming', 'xbox', 'playstation', 'nintendo', 'games', 'truegaming',
            'Steam', 'GamingLeaksAndRumours', 'GameDeals', 'patientgamers', 'ShouldIbuythisgame',
            'PS5', 'XboxSeriesX', 'NintendoSwitch', 'SteamDeck', 'GameStop'
        ]
    
    logger.info(f"Calculating Reddit sentiment signals for {len(tickers)} tickers")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Subreddits: {', '.join(subreddits)}")
    logger.info(f"Reddit API available: {reddit_client.is_available()}")
    
    # Calculate expected number of signals (every date * every ticker)
    expected_signals = len(tickers) * ((end_date - start_date).days + 1)
    logger.info(f"Expected signals: {expected_signals} (guaranteed coverage for all date-ticker combinations)")
    
    # Fetch Reddit posts and analyze sentiment
    posts_df = get_reddit_posts(subreddits, tickers, start_date, end_date)
    
    # Process sentiment for each post
    if not posts_df.empty:
        posts_df['sentiment_score'] = posts_df['content'].apply(analyze_sentiment)
        posts_df['sentiment_score'] = posts_df['sentiment_score'].fillna(0.0)
    else:
        logger.warning("No Reddit posts found - will use fallback scoring")
    
    # Generate signals for EVERY date-ticker combination
    signals_data = []
    current_date = start_date
    
    while current_date <= end_date:
        for ticker in tickers:
            # Always calculate a score, even if no posts exist for this date/ticker
            aggregated_score = aggregate_sentiment_scores(posts_df, ticker, current_date)
            
            # Determine calculation method and post count
            if posts_df.empty or len(posts_df[posts_df['ticker'] == ticker]) == 0:
                calculation_method = 'fallback_no_data'
                posts_analyzed = 0
            else:
                calculation_method = 'textblob_with_gaming_keywords'
                posts_analyzed = len(posts_df[posts_df['ticker'] == ticker])
            
            # Calculate confidence based on posts analyzed
            confidence = min(posts_analyzed / 50.0, 1.0) if posts_analyzed > 0 else 0.0
            
            signals_data.append({
                'asof_date': current_date,
                'ticker': ticker,
                'signal_name': 'SENTIMENT_RDDT',
                'value': aggregated_score,
                'confidence': confidence,
                'posts_analyzed': posts_analyzed,
                'calculation_method': calculation_method,
                'metadata': {
                    'subreddits_analyzed': subreddits,
                    'data_source': 'reddit_api',
                    'has_data': posts_analyzed > 0
                }
            })
        
        current_date += timedelta(days=1)
    
    # Convert to DataFrame for easier handling
    signals_df = pd.DataFrame(signals_data)
    
    # Verify we have the expected number of signals
    actual_signals = len(signals_df)
    if actual_signals != expected_signals:
        logger.error(f"Signal count mismatch! Expected: {expected_signals}, Actual: {actual_signals}")
        raise ValueError(f"Signal count mismatch! Expected: {expected_signals}, Actual: {actual_signals}")
    
    # Calculate summary statistics
    summary_stats = {
        'total_signals': len(signals_df),
        'unique_tickers': signals_df['ticker'].nunique(),
        'date_range_days': (end_date - start_date).days + 1,
        'average_sentiment': signals_df['value'].mean(),
        'sentiment_std': signals_df['value'].std(),
        'min_sentiment': signals_df['value'].min(),
        'max_sentiment': signals_df['value'].max(),
        'total_posts_analyzed': len(posts_df) if not posts_df.empty else 0,
        'signals_with_data': len(signals_df[signals_df['metadata'].apply(lambda x: x.get('has_data', False))]),
        'signals_fallback': len(signals_df[signals_df['metadata'].apply(lambda x: x.get('calculation_method', '') == 'fallback_no_data')]),
        'coverage_guaranteed': True,
        'reddit_api_used': True
    }
    
    result = {
        'signals_df': signals_df,
        'signals_count': len(signals_df),
        'summary_stats': summary_stats,
        'parameters': {
            'tickers': tickers,
            'start_date': start_date,
            'end_date': end_date,
            'subreddits': subreddits
        },
        'status': 'implemented_with_reddit_api'
    }
    
    logger.info(f"Generated {len(signals_df)} sentiment signals (100% coverage)")
    logger.info(f"Average sentiment: {summary_stats['average_sentiment']:.3f}")
    logger.info(f"Signals with data: {summary_stats['signals_with_data']}")
    logger.info(f"Fallback signals: {summary_stats['signals_fallback']}")
    logger.info(f"Reddit API used: {summary_stats['reddit_api_used']}")
    
    return result


def get_reddit_posts(subreddits: List[str], 
                     tickers: List[str], 
                     start_date: date, 
                     end_date: date) -> pd.DataFrame:
    """
    Fetch Reddit posts related to specified tickers from given subreddits.
    
    This function searches for posts containing ticker-related keywords
    and returns relevant post data for sentiment analysis.
    
    Args:
        subreddits: List of subreddit names to search
        tickers: List of ticker symbols to search for
        start_date: Start date for post filtering
        end_date: End date for post filtering
        
    Returns:
        DataFrame containing Reddit post data
    """
    logger.info(f"Fetching Reddit posts for {len(tickers)} tickers from {len(subreddits)} subreddits")
    
    # Use Reddit API (will fail if not available)
    return _fetch_real_reddit_posts(subreddits, tickers, start_date, end_date)


def _fetch_real_reddit_posts(subreddits: List[str], 
                            tickers: List[str], 
                            start_date: date, 
                            end_date: date) -> pd.DataFrame:
    """Fetch real posts from Reddit API."""
    posts_data = []
    
    try:
        for ticker in tickers:
            # Get keywords for this ticker
            keywords = TICKER_GAME_MAPPING.get(ticker, [ticker.lower()])
            
            # Search for posts
            posts = reddit_client.search_posts(
                subreddits=subreddits,
                keywords=keywords,
                limit=100,  # Increased limit per ticker
                time_filter='month',  # Extended time filter to capture more posts
                ticker=ticker  # Pass the ticker to ensure correct assignment
            )
            
            # Filter posts by date range
            for post in posts:
                post_date = datetime.fromtimestamp(post['created_utc']).date()
                if start_date <= post_date <= end_date:
                    posts_data.append(post)
            
            logger.info(f"Found {len([p for p in posts_data if p['ticker'] == ticker])} posts for {ticker}")
            
    except Exception as e:
        logger.error(f"Error fetching Reddit posts: {e}")
        raise ConnectionError(f"Failed to fetch Reddit posts: {e}")
    
    if not posts_data:
        logger.warning("No Reddit posts found for the specified criteria")
        return pd.DataFrame()  # Return empty DataFrame instead of mock data
    
    return pd.DataFrame(posts_data)


def analyze_sentiment(text: str) -> float:
    """
    Analyze sentiment of text content using TextBlob and gaming-specific keywords.
    
    This function combines TextBlob's sentiment analysis with gaming-specific
    keyword detection to provide more accurate sentiment scoring for gaming content.
    
    Args:
        text: Text content to analyze
        
    Returns:
        Sentiment score between -1 (negative) and 1 (positive)
    """
    if not text or not isinstance(text, str):
        return 0.0
    
    try:
        # Clean and preprocess text
        cleaned_text = _clean_text(text)
        
        # Get TextBlob sentiment score
        blob = TextBlob(cleaned_text)
        textblob_score = blob.sentiment.polarity
        
        # Get gaming-specific keyword sentiment
        keyword_score = _calculate_gaming_keyword_sentiment(cleaned_text)
        
        # Combine TextBlob and keyword scores (weighted average)
        # TextBlob gets 70% weight, keywords get 30% weight
        combined_score = (0.7 * textblob_score) + (0.3 * keyword_score)
        
        # Normalize to ensure score is between -1 and 1
        final_score = max(-1.0, min(1.0, combined_score))
        
        return final_score
        
    except Exception as e:
        logger.warning(f"Error analyzing sentiment for text: {e}")
        return 0.0


def aggregate_sentiment_scores(sentiment_data: pd.DataFrame, 
                              ticker: str, 
                              target_date: date) -> float:
    """
    Aggregate sentiment scores for a specific ticker and date.
    
    This function combines multiple sentiment scores from different
    Reddit posts/comments into a single aggregated score for the ticker.
    If no data exists, returns a fallback score to ensure coverage.
    
    Args:
        sentiment_data: DataFrame containing sentiment scores
        ticker: Ticker symbol to aggregate for
        target_date: Date to aggregate for
        
    Returns:
        Aggregated sentiment score (always returns a value)
    """
    # Filter data for the specific ticker
    ticker_data = sentiment_data[sentiment_data['ticker'] == ticker] if not sentiment_data.empty else pd.DataFrame()
    
    if ticker_data.empty:
        # Fallback: return neutral score of 0.00 when no data found
        # This ensures every date-ticker combination has a score
        logger.debug(f"Using neutral score 0.00 for {ticker} on {target_date} (no data found)")
        return 0.0
    
    # Create a copy to avoid SettingWithCopyWarning
    ticker_data = ticker_data.copy()
    
    # Weight by post engagement (score + comments)
    ticker_data['engagement_weight'] = (
        ticker_data['score'] + (ticker_data['num_comments'] * 2)
    )
    
    # Apply time decay (more recent posts get higher weight)
    current_time = datetime.now().timestamp()
    ticker_data['time_weight'] = 1.0 / (1.0 + (current_time - ticker_data['created_utc']) / 86400)
    
    # Calculate final weights
    ticker_data['final_weight'] = ticker_data['engagement_weight'] * ticker_data['time_weight']
    
    # Weighted average sentiment
    if ticker_data['final_weight'].sum() == 0:
        return ticker_data['sentiment_score'].mean()
    
    weighted_sentiment = (
        (ticker_data['sentiment_score'] * ticker_data['final_weight']).sum() / 
        ticker_data['final_weight'].sum()
    )
    
    return weighted_sentiment


def _clean_text(text: str) -> str:
    """Clean and preprocess text for sentiment analysis."""
    # Convert to lowercase
    text = text.lower()
    
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # Remove special characters but keep spaces
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def _calculate_gaming_keyword_sentiment(text: str) -> float:
    """Calculate sentiment based on gaming-specific keywords."""
    words = text.split()
    
    positive_count = sum(1 for word in words if word in GAMING_POSITIVE_KEYWORDS)
    negative_count = sum(1 for word in words if word in GAMING_NEGATIVE_KEYWORDS)
    
    total_keywords = positive_count + negative_count
    
    if total_keywords == 0:
        return 0.0
    
    # Calculate sentiment ratio
    sentiment_ratio = (positive_count - negative_count) / total_keywords
    
    # Scale to -1 to 1 range
    return max(-1.0, min(1.0, sentiment_ratio))


# Example usage and testing functions

def run_example():
    """Run an example signal calculation to demonstrate functionality."""
    print("Running Reddit sentiment signal calculation example...")
    
    # Example parameters
    tickers = ['EA', 'MSFT', 'RBLX']
    start_date = date.today() - timedelta(days=7)
    end_date = date.today()
    subreddits = ['gaming', 'pcgaming']
    
    # Calculate signals
    result = calculate_signals(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        subreddits=subreddits
    )
    
    print(f"Generated {result['signals_count']} signals")
    print(f"Average sentiment: {result['summary_stats']['average_sentiment']:.3f}")
    print(f"Total posts analyzed: {result['summary_stats']['total_posts_analyzed']}")
    print(f"Signals with data: {result['summary_stats']['signals_with_data']}")
    print(f"Fallback signals: {result['summary_stats']['signals_fallback']}")
    print(f"Coverage guaranteed: {result['summary_stats']['coverage_guaranteed']}")
    print(f"Reddit API used: {result['summary_stats']['reddit_api_used']}")
    
    # Verify coverage
    signals_df = result['signals_df']
    expected_combinations = len(tickers) * ((end_date - start_date).days + 1)
    actual_combinations = len(signals_df)
    
    print(f"\nCoverage verification:")
    print(f"Expected date-ticker combinations: {expected_combinations}")
    print(f"Actual signals generated: {actual_combinations}")
    print(f"Coverage complete: {expected_combinations == actual_combinations}")
    
    return result


if __name__ == "__main__":
    # Run example when script is executed directly
    run_example()
