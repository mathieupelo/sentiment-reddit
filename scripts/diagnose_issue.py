#!/usr/bin/env python3
"""Diagnose the issues with EA processing and the error."""

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

def diagnose():
    """Diagnose the issues."""
    conn = get_ore_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Check if processing log table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'tin' AND table_name = 'reddit_processing_log'
                )
            """, prepare=False)
            table_exists = cur.fetchone()[0]
            print(f"1. Processing log table exists: {table_exists}")
            
            if table_exists:
                # Count all entries
                cur.execute(f"SELECT COUNT(*) FROM tin.reddit_processing_log", prepare=False)
                total_count = cur.fetchone()[0]
                print(f"2. Total entries in processing log: {total_count}")
                
                # Check for EA on 2025-11-28
                cur.execute("""
                    SELECT 
                        ticker,
                        processed_date,
                        status,
                        posts_found,
                        posts_inserted,
                        processing_started_at,
                        processing_completed_at
                    FROM tin.reddit_processing_log
                    WHERE ticker = 'EA' AND processed_date = '2025-11-28'
                """, prepare=False)
                ea_entries = cur.fetchall()
                
                print(f"\n3. EA entries for 2025-11-28: {len(ea_entries)}")
                for entry in ea_entries:
                    print(f"   - Status: {entry[2]}, Found: {entry[3]}, Inserted: {entry[4]}")
                    print(f"     Started: {entry[5]}, Completed: {entry[6]}")
                
                # Check for any 'in_progress' entries (stale)
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM tin.reddit_processing_log
                    WHERE status = 'in_progress'
                """, prepare=False)
                in_progress_count = cur.fetchone()[0]
                print(f"\n4. Stale 'in_progress' entries: {in_progress_count}")
                
                if in_progress_count > 0:
                    cur.execute("""
                        SELECT ticker, processed_date, processing_started_at
                        FROM tin.reddit_processing_log
                        WHERE status = 'in_progress'
                        ORDER BY processing_started_at DESC
                    """, prepare=False)
                    stale_entries = cur.fetchall()
                    print("   Stale entries:")
                    for entry in stale_entries:
                        print(f"     - {entry[0]} on {entry[1]} (started: {entry[2]})")
            
            # Check reddit_posts table
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'tin' AND table_name = 'reddit_posts'
                )
            """, prepare=False)
            posts_table_exists = cur.fetchone()[0]
            print(f"\n5. Reddit posts table exists: {posts_table_exists}")
            
            if posts_table_exists:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM tin.reddit_posts
                    WHERE ticker = 'EA' AND DATE(created_datetime) = '2025-11-28'
                """, prepare=False)
                ea_posts_count = cur.fetchone()[0]
                print(f"6. EA posts in database for 2025-11-28: {ea_posts_count}")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Error during diagnosis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    diagnose()


