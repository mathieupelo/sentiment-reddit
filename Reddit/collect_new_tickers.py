#!/usr/bin/env python3
"""Collect Reddit posts for the 4 new tickers: SQNXF, KSFTF, KNMCY, NEXOY"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from reddit_mysql_collector import RedditMySQLCollector

def main():
    print("=" * 60)
    print("COLLECTING REDDIT POSTS FOR NEW TICKERS")
    print("=" * 60)
    
    # Initialize collector
    collector = RedditMySQLCollector(
        host='localhost',
        user='root', 
        password='3421',
        database='reddit_sentiment'
    )
    
    # Connect to MySQL
    print("\nConnecting to MySQL database...")
    if not collector.connect():
        print("ERROR: Failed to connect to MySQL")
        return 1
    
    print("Connected successfully!")
    
    # Define tickers and subreddits
    tickers = ['SQNXF', 'KSFTF', 'KNMCY', 'NEXOY']
    subreddits = [
        'gaming', 'pcgaming', 'xbox', 'playstation', 'nintendo', 'games', 'truegaming',
        'Steam', 'GamingLeaksAndRumours', 'GameDeals', 'patientgamers', 'ShouldIbuythisgame',
        'PS5', 'XboxSeriesX', 'NintendoSwitch', 'SteamDeck', 'GameStop',
        'investing', 'stocks', 'wallstreetbets', 'technology'
    ]
    
    print(f"\nTickers to collect: {', '.join(tickers)}")
    print(f"Subreddits: {len(subreddits)} gaming and finance subreddits")
    print("\nStarting collection...\n")
    
    try:
        # Collect posts
        collector.collect_posts(
            tickers=tickers,
            subreddits=subreddits,
            limit_per_search=100
        )
        
        print("\n" + "=" * 60)
        print("COLLECTION COMPLETE!")
        print("=" * 60)
        
        # Disconnect
        collector.disconnect()
        print("\nDisconnected from MySQL")
        return 0
        
    except Exception as e:
        print(f"\nERROR during collection: {e}")
        collector.disconnect()
        return 1

if __name__ == "__main__":
    sys.exit(main())


