#!/usr/bin/env python3
"""
List all keywords for all companies to verify they are in the database.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
import psycopg

# Load environment variables
env_path = Path(__file__).parent.parent.parent / 'Alpha-Crucible-Quant' / '.env'
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        load_dotenv()

def get_main_db_connection():
    """Get connection to main database."""
    try:
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            return psycopg.connect(database_url)
        
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '5432')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_NAME')
        
        if not all([host, user, password, database]):
            raise ValueError("Database connection parameters not found")
        
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def list_all_keywords(conn):
    """List all keywords grouped by company."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                t.ticker,
                COALESCE(ci.name, 'Unknown') as company_name,
                ck.keyword,
                COALESCE((ck.metadata->>'priority')::int, 999) as priority
            FROM varrock.company_keywords ck
            JOIN varrock.tickers t ON ck.company_uid = t.company_uid
            LEFT JOIN varrock.company_info ci ON ck.company_uid = ci.company_uid
            ORDER BY t.ticker, priority ASC
        """)
        rows = cur.fetchall()
    
    if not rows:
        print("No keywords found in database!")
        return
    
    # Group by ticker
    companies = {}
    for ticker, company_name, keyword, priority in rows:
        if ticker not in companies:
            companies[ticker] = {
                'name': company_name,
                'keywords': []
            }
        companies[ticker]['keywords'].append((keyword, priority))
    
    print("=" * 80)
    print("KEYWORDS BY COMPANY")
    print("=" * 80)
    print()
    
    for ticker in sorted(companies.keys()):
        company = companies[ticker]
        print(f"Company: {company['name']} ({ticker})")
        print(f"  Total Keywords: {len(company['keywords'])}")
        print("  Keywords (by priority):")
        for keyword, priority in company['keywords']:
            print(f"    [{priority}] {keyword}")
        print()

def main():
    try:
        conn = get_main_db_connection()
        list_all_keywords(conn)
        conn.close()
        return 0
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())



