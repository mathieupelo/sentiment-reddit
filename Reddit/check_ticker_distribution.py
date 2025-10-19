#!/usr/bin/env python3
"""Check how posts are distributed across tickers."""

import mysql.connector

def main():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='3421',
        database='reddit_sentiment'
    )
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT ticker, COUNT(*) as count 
        FROM reddit_posts 
        GROUP BY ticker 
        ORDER BY ticker
    """)
    
    print("\nPosts per ticker:")
    print("-" * 40)
    
    total = 0
    for ticker, count in cursor.fetchall():
        print(f"{ticker}: {count} posts")
        total += count
    
    print("-" * 40)
    print(f"Total: {total} posts")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()


