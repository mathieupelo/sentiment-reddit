"""
MySQL Collector for Reddit Posts
Fetches Reddit posts and stores them in a MySQL database for verification and analysis.
"""

import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime, date
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.calculate_signals import RedditAPIClient, TICKER_GAME_MAPPING

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RedditMySQLCollector:
    """Collects Reddit posts and stores them in MySQL database."""
    
    def __init__(self, host='localhost', user='root', password='', database='reddit_sentiment'):
        """Initialize MySQL connection."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.reddit_client = None
        
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
    
    def create_session(self, session_name: str, tickers: List[str], subreddits: List[str]) -> int:
        """Create a new collection session."""
        try:
            cursor = self.connection.cursor()
            query = """
                INSERT INTO collection_sessions 
                (session_name, start_time, tickers, subreddits, status)
                VALUES (%s, %s, %s, %s, %s)
            """
            import json
            cursor.execute(query, (
                session_name,
                datetime.now(),
                json.dumps(tickers),
                json.dumps(subreddits),
                'running'
            ))
            self.connection.commit()
            session_id = cursor.lastrowid
            logger.info(f"Created collection session: {session_name} (ID: {session_id})")
            return session_id
        except Error as e:
            logger.error(f"Error creating session: {e}")
            return None
    
    def update_session(self, session_id: int, status: str, total_posts: int, error_msg: str = None):
        """Update collection session status."""
        try:
            cursor = self.connection.cursor()
            query = """
                UPDATE collection_sessions 
                SET end_time = %s, status = %s, total_posts_collected = %s, error_message = %s
                WHERE id = %s
            """
            cursor.execute(query, (datetime.now(), status, total_posts, error_msg, session_id))
            self.connection.commit()
            logger.info(f"Updated session {session_id}: {status}, {total_posts} posts")
        except Error as e:
            logger.error(f"Error updating session: {e}")
    
    def store_post(self, post: Dict, ticker: str, keyword: str) -> bool:
        """Store a single Reddit post in the database."""
        try:
            cursor = self.connection.cursor()
            
            # Convert Unix timestamp to datetime
            created_dt = datetime.fromtimestamp(post['created_utc'])
            
            query = """
                INSERT INTO reddit_posts 
                (reddit_id, title, content, author, subreddit, created_utc, created_date, 
                 created_datetime, score, num_comments, url, is_self, ticker, keyword_matched)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                score = VALUES(score),
                num_comments = VALUES(num_comments)
            """
            
            cursor.execute(query, (
                post['id'],
                post['title'],
                post['content'],
                post['author'],
                post['subreddit'],
                int(post['created_utc']),
                created_dt.date(),
                created_dt,
                post['score'],
                post['num_comments'],
                post['url'],
                post['is_self'],
                ticker,
                keyword
            ))
            
            self.connection.commit()
            return True
            
        except Error as e:
            logger.warning(f"Error storing post {post.get('id')}: {e}")
            return False
    
    def collect_posts(self, tickers: List[str], subreddits: List[str], limit_per_search: int = 100):
        """
        Collect Reddit posts for specified tickers and subreddits.
        
        Args:
            tickers: List of stock tickers
            subreddits: List of subreddit names
            limit_per_search: Max posts per keyword search
        """
        # Initialize Reddit client
        self.reddit_client = RedditAPIClient()
        
        # Create collection session
        session_name = f"Collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        session_id = self.create_session(session_name, tickers, subreddits)
        
        if not session_id:
            logger.error("Failed to create collection session")
            return
        
        total_posts_collected = 0
        
        try:
            logger.info("=" * 60)
            logger.info("STARTING REDDIT POST COLLECTION")
            logger.info("=" * 60)
            logger.info(f"Tickers: {', '.join(tickers)}")
            logger.info(f"Subreddits: {', '.join(subreddits)}")
            logger.info(f"Limit per search: {limit_per_search}")
            logger.info("=" * 60)
            
            for i, ticker in enumerate(tickers, 1):
                logger.info(f"\n[{i}/{len(tickers)}] Processing ticker: {ticker}")
                
                # Get keywords for this ticker
                keywords = TICKER_GAME_MAPPING.get(ticker, [ticker.lower()])
                logger.info(f"Keywords: {', '.join(keywords)}")
                
                ticker_posts = 0
                
                for keyword in keywords:
                    logger.info(f"  Searching for '{keyword}'...")
                    
                    try:
                        # Fetch posts from Reddit API
                        posts = self.reddit_client.search_posts(
                            subreddits=subreddits,
                            keywords=[keyword],
                            limit=limit_per_search,
                            time_filter='all',  # Get all available posts
                            ticker=ticker
                        )
                        
                        logger.info(f"  Found {len(posts)} posts for '{keyword}'")
                        
                        # Store posts in database
                        stored_count = 0
                        for post in posts:
                            if self.store_post(post, ticker, keyword):
                                stored_count += 1
                        
                        logger.info(f"  Stored {stored_count}/{len(posts)} posts in database")
                        ticker_posts += stored_count
                        
                    except Exception as e:
                        logger.error(f"  Error searching for '{keyword}': {e}")
                        continue
                
                total_posts_collected += ticker_posts
                logger.info(f"Total posts collected for {ticker}: {ticker_posts}")
            
            # Update session as completed
            self.update_session(session_id, 'completed', total_posts_collected)
            
            logger.info("\n" + "=" * 60)
            logger.info("COLLECTION COMPLETED")
            logger.info(f"Total posts collected: {total_posts_collected}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            self.update_session(session_id, 'failed', total_posts_collected, str(e))
    
    def verify_data(self):
        """Verify collected data and print statistics."""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Total posts
            cursor.execute("SELECT COUNT(*) as total FROM reddit_posts")
            total = cursor.fetchone()['total']
            
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
            
            # Posts by date (sample last 30 days)
            cursor.execute("""
                SELECT created_date, COUNT(*) as count 
                FROM reddit_posts 
                WHERE created_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                GROUP BY created_date 
                ORDER BY created_date DESC
                LIMIT 10
            """)
            by_date = cursor.fetchall()
            
            print("\n" + "=" * 60)
            print("DATABASE VERIFICATION")
            print("=" * 60)
            print(f"Total posts: {total}")
            print(f"\nDate range: {date_range['min_date']} to {date_range['max_date']}")
            print(f"\nPosts by ticker:")
            for row in by_ticker:
                print(f"  {row['ticker']:6}: {row['count']:5} posts")
            print(f"\nRecent posts by date (last 10 days with posts):")
            for row in by_date:
                print(f"  {row['created_date']}: {row['count']:4} posts")
            print("=" * 60)
            
        except Error as e:
            logger.error(f"Error verifying data: {e}")


def main():
    """Main function to run the collector."""
    print("=" * 60)
    print("REDDIT MYSQL COLLECTOR")
    print("=" * 60)
    print()
    
    # Configuration
    tickers = ['EA', 'TTWO', 'NTES', 'RBLX', 'MSFT', 'SONY', 
               'WBD', 'NCBDY', 'GDEV', 'OTGLF', 'SNAL', 'GRVY']
    
    subreddits = [
        'gaming', 'pcgaming', 'xbox', 'playstation', 'nintendo', 'games', 'truegaming',
        'Steam', 'GamingLeaksAndRumours', 'GameDeals', 'patientgamers', 'ShouldIbuythisgame',
        'PS5', 'XboxSeriesX', 'NintendoSwitch', 'SteamDeck', 'GameStop'
    ]
    
    print(f"Tickers: {', '.join(tickers)}")
    print(f"Subreddits: {len(subreddits)} gaming subreddits")
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
    
    # Create collector
    collector = RedditMySQLCollector(host=host, user=user, password=password, database=database)
    
    # Connect to database
    if not collector.connect():
        print("Failed to connect to MySQL. Exiting.")
        return
    
    print("Connected to MySQL successfully!")
    print()
    
    # Confirm before proceeding
    response = input("Proceed with Reddit post collection? (y/N): ").strip().lower()
    if response != 'y':
        print("Collection cancelled.")
        collector.disconnect()
        return
    
    # Collect posts
    collector.collect_posts(tickers, subreddits, limit_per_search=100)
    
    # Verify data
    print()
    response = input("Show data verification? (y/N): ").strip().lower()
    if response == 'y':
        collector.verify_data()
    
    # Disconnect
    collector.disconnect()
    print("\nDone!")


if __name__ == "__main__":
    main()