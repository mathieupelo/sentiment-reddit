#!/usr/bin/env python3
"""Check EA status in database."""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    load_dotenv()

def get_ore_db_connection():
    """Get connection to ORE database."""
    try:
        database_url = os.getenv('ORE_DATABASE_URL')
        if not database_url:
            database_url = os.getenv('DATABASE_ORE_URL')
        
        if database_url:
            return psycopg.connect(database_url)
        
        host = os.getenv('ORE_DB_HOST')
        port = os.getenv('ORE_DB_PORT', '5432')
        user = os.getenv('ORE_DB_USER')
        password = os.getenv('ORE_DB_PASSWORD')
        database = os.getenv('ORE_DB_NAME')
        
        if not all([host, user, password, database]):
            raise ValueError("ORE database connection parameters not found")
        
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
    except Exception as e:
        print(f"Error connecting to ORE database: {e}")
        raise

def check_ea_status(conn):
    """Check EA status."""
    with conn.cursor() as cur:
        # Check processing log
        cur.execute("""
            SELECT 
                ticker,
                processed_date,
                posts_found,
                posts_inserted,
                status,
                processing_started_at,
                processing_completed_at,
                error_message
            FROM tin.reddit_processing_log
            WHERE ticker = 'EA' AND processed_date = '2025-11-28'
            ORDER BY processing_started_at DESC
        """, prepare=False)
        log_entries = cur.fetchall()
        
        print("EA Processing Log Entries for 2025-11-28:")
        print("=" * 80)
        if log_entries:
            for entry in log_entries:
                ticker, date, found, inserted, status, started, completed, error = entry
                print(f"Status: {status}")
                print(f"Posts Found: {found}")
                print(f"Posts Inserted: {inserted}")
                print(f"Started: {started}")
                print(f"Completed: {completed}")
                if error:
                    print(f"Error: {error}")
                print()
        else:
            print("No log entries found")
        
        # Check actual posts
        cur.execute("""
            SELECT COUNT(*) 
            FROM tin.reddit_posts
            WHERE ticker = 'EA' AND DATE(created_datetime) = '2025-11-28'
        """, prepare=False)
        post_count = cur.fetchone()[0]
        
        print(f"\nActual posts in database for EA on 2025-11-28: {post_count}")
        
        if post_count > 0:
            cur.execute("""
                SELECT 
                    reddit_id,
                    title,
                    keyword_matched,
                    upvotes
                FROM tin.reddit_posts
                WHERE ticker = 'EA' AND DATE(created_datetime) = '2025-11-28'
                ORDER BY upvotes DESC
                LIMIT 5
            """, prepare=False)
            sample_posts = cur.fetchall()
            print("\nSample posts:")
            for post in sample_posts:
                print(f"  - {post[1][:50]}... (upvotes: {post[3]}, keyword: {post[2]})")

if __name__ == "__main__":
    try:
        conn = get_ore_db_connection()
        check_ea_status(conn)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


