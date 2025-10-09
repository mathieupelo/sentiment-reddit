#!/usr/bin/env python3
"""Check how many posts we have for the new tickers."""

import mysql.connector

def main():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='3421',
        database='reddit_sentiment'
    )
    
    cursor = conn.cursor()
    
    tickers = ['SQNXF', 'KSFTF', 'KNMCY', 'NEXOY']
    
    print("\nCurrent post counts for new tickers:")
    print("-" * 40)
    
    for ticker in tickers:
        cursor.execute(
            "SELECT COUNT(*) FROM reddit_posts WHERE ticker = %s",
            (ticker,)
        )
        count = cursor.fetchone()[0]
        print(f"{ticker}: {count} posts")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()

