#!/usr/bin/env python3
"""Check the processing log table to see what's been processed."""

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

def check_processing_log(conn):
    """Check what's in the processing log."""
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'tin' AND table_name = 'reddit_processing_log'
            )
        """)
        exists = cur.fetchone()[0]
        print(f"Processing log table exists: {exists}")
        
        if not exists:
            print("Table doesn't exist yet!")
            return
        
        # Get all entries
        cur.execute("""
            SELECT 
                ticker,
                processed_date,
                posts_found,
                posts_inserted,
                status,
                processing_started_at,
                processing_completed_at
            FROM tin.reddit_processing_log
            ORDER BY processing_started_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        
        print(f"\nTotal entries in processing log: {len(rows)}")
        print("\nRecent processing log entries:")
        print("=" * 100)
        for row in rows:
            ticker, date, found, inserted, status, started, completed = row
            print(f"Ticker: {ticker:6} | Date: {date} | Found: {found:4} | Inserted: {inserted:4} | Status: {status:12} | Started: {started}")
        
        # Check specifically for EA on 2025-11-28
        cur.execute("""
            SELECT *
            FROM tin.reddit_processing_log
            WHERE ticker = 'EA' AND processed_date = '2025-11-28'
        """)
        ea_rows = cur.fetchall()
        
        print(f"\nEA entries for 2025-11-28: {len(ea_rows)}")
        if ea_rows:
            print("EA processing log entries:")
            for row in ea_rows:
                print(f"  {row}")

if __name__ == "__main__":
    try:
        conn = get_ore_db_connection()
        check_processing_log(conn)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



