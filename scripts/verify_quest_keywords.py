#!/usr/bin/env python3
"""Verify keywords for QUEST universe companies."""

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

def verify_keywords(conn):
    """Verify keywords for all QUEST universe companies."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                ci.name as company_name,
                mt.ticker as main_ticker,
                COUNT(ck.id) as keyword_count,
                ARRAY_AGG(ck.keyword ORDER BY (ck.metadata->>'priority')::int) as keywords
            FROM universe_companies uc
            JOIN varrock.companies c ON uc.company_uid = c.company_uid
            LEFT JOIN varrock.company_info ci ON c.company_uid = ci.company_uid
            LEFT JOIN varrock.tickers mt ON c.company_uid = mt.company_uid AND mt.is_main_ticker = TRUE
            LEFT JOIN varrock.company_keywords ck ON c.company_uid = ck.company_uid
            WHERE uc.universe_id = 491
            GROUP BY ci.name, mt.ticker
            ORDER BY ci.name, mt.ticker
        """, prepare=False)
        
        results = cur.fetchall()
        
        print("=" * 80)
        print("QUEST Universe Keywords Verification")
        print("=" * 80)
        print()
        
        companies_with_few_keywords = []
        total_keywords = 0
        
        for company_name, ticker, keyword_count, keywords in results:
            total_keywords += keyword_count or 0
            keyword_list = list(keywords) if keywords else []
            
            status = "OK" if (keyword_count or 0) >= 5 else "LOW"
            if (keyword_count or 0) < 5:
                companies_with_few_keywords.append((company_name, ticker, keyword_count or 0))
            
            print(f"{status:3} | {company_name:45} | {ticker:12} | {keyword_count or 0:2} keywords")
            if keyword_list:
                for i, kw in enumerate(keyword_list[:5], 1):
                    try:
                        print(f"      [{i}] {kw}")
                    except UnicodeEncodeError:
                        print(f"      [{i}] (keyword with special characters)")
                if len(keyword_list) > 5:
                    print(f"      ... and {len(keyword_list) - 5} more")
            print()
        
        print("=" * 80)
        print(f"Summary:")
        print(f"  Total companies: {len(results)}")
        print(f"  Total keywords: {total_keywords}")
        print(f"  Average keywords per company: {total_keywords / len(results):.1f}")
        print(f"  Companies with < 5 keywords: {len(companies_with_few_keywords)}")
        if companies_with_few_keywords:
            print(f"\n  Companies needing more keywords:")
            for name, ticker, count in companies_with_few_keywords:
                print(f"    - {name} ({ticker}): {count} keywords")
        print("=" * 80)

if __name__ == "__main__":
    try:
        conn = get_main_db_connection()
        verify_keywords(conn)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

