#!/usr/bin/env python3
"""
Insert keywords for GameCore-8 universe companies into varrock.company_keywords table.

This script:
1. Fetches companies from GameCore-8 universe
2. Inserts 5 unique keywords per company with priorities 1-5
3. Keywords are designed to be unique to avoid false matches
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

# Keywords for each company (ticker -> list of (keyword, priority))
# Priority 1 = highest priority (most important)
KEYWORDS_BY_TICKER = {
    "NCBDY": [
        ("Bandai Namco", 1),
        ("Tekken", 2),
        ("Dark Souls", 3),
        ("Elden Ring", 4),
        ("Tales of", 5),
    ],
    "OTGLF": [
        ("CD Projekt", 1),
        ("Witcher", 2),
        ("Cyberpunk 2077", 3),
        ("GOG", 4),
        ("CDPR", 5),
    ],
    "EA": [
        ("Electronic Arts", 1),
        ("FIFA", 2),
        ("Madden", 3),
        ("Apex Legends", 4),
        ("Battlefield", 5),
    ],
    "NTES": [
        ("NetEase", 1),
        ("Diablo Immortal", 2),
        ("Onmyoji", 3),
        ("Identity V", 4),
        ("Knives Out", 5),
    ],
    "RBLX": [
        ("Roblox", 1),
        ("Robux", 2),
        ("Roblox Studio", 3),
        ("Roblox game", 4),
        ("Roblox developer", 5),
    ],
    "SONY": [
        ("PlayStation", 1),
        ("PS5", 2),
        ("PS4", 3),
        ("PlayStation Studios", 4),
        ("Sony Interactive", 5),
    ],
    "TTWO": [
        ("Take-Two", 1),
        ("GTA", 2),
        ("Grand Theft Auto", 3),
        ("Rockstar Games", 4),
        ("2K Games", 5),
    ],
    "WBD": [
        ("Warner Bros Games", 1),
        ("Mortal Kombat", 2),
        ("NetherRealm", 3),
        ("WB Games", 4),
        ("Hogwarts Legacy", 5),
    ],
}


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


def get_gamecore8_companies(conn):
    """Get companies from GameCore-8 universe."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                uc.company_uid,
                ci.name as company_name,
                mt.ticker as main_ticker
            FROM universe_companies uc
            JOIN varrock.companies c ON uc.company_uid = c.company_uid
            LEFT JOIN varrock.company_info ci ON c.company_uid = ci.company_uid
            LEFT JOIN varrock.tickers mt ON c.company_uid = mt.company_uid AND mt.is_main_ticker = TRUE
            WHERE uc.universe_id = (
                SELECT id FROM universes WHERE name LIKE '%GameCore-8%' OR name LIKE '%GC-8%' LIMIT 1
            )
            ORDER BY ci.name, mt.ticker
        """)
        return cur.fetchall()


def insert_keywords(conn, company_uid: str, ticker: str, keywords: list):
    """Insert keywords for a company."""
    inserted = 0
    skipped = 0
    
    with conn.cursor() as cur:
        for keyword, priority in keywords:
            # Check if keyword already exists for this company
            cur.execute("""
                SELECT id FROM varrock.company_keywords
                WHERE company_uid = %s AND keyword = %s
            """, (company_uid, keyword))
            
            if cur.fetchone():
                print(f"  [SKIP] Existing keyword: '{keyword}' (priority {priority})")
                skipped += 1
                continue
            
            # Insert keyword with metadata
            metadata = json.dumps({"priority": priority})
            cur.execute("""
                INSERT INTO varrock.company_keywords (company_uid, keyword, metadata)
                VALUES (%s, %s, %s::jsonb)
            """, (company_uid, keyword, metadata))
            inserted += 1
            print(f"  [OK] Inserted: '{keyword}' (priority {priority})")
    
    return inserted, skipped


def main():
    """Main function to insert keywords."""
    print("=" * 60)
    print("Inserting Keywords for GameCore-8 Universe")
    print("=" * 60)
    
    try:
        conn = get_main_db_connection()
        companies = get_gamecore8_companies(conn)
        
        if not companies:
            print("No companies found in GameCore-8 universe!")
            return 1
        
        print(f"\nFound {len(companies)} companies in GameCore-8 universe\n")
        
        total_inserted = 0
        total_skipped = 0
        
        for company_uid, company_name, ticker in companies:
            if not ticker:
                print(f"[WARN] Skipping {company_name}: No ticker found")
                continue
            
            if ticker not in KEYWORDS_BY_TICKER:
                print(f"[WARN] Skipping {company_name} ({ticker}): No keywords defined")
                continue
            
            print(f"Processing {company_name} ({ticker}):")
            keywords = KEYWORDS_BY_TICKER[ticker]
            inserted, skipped = insert_keywords(conn, company_uid, ticker, keywords)
            total_inserted += inserted
            total_skipped += skipped
            print()
        
        conn.commit()
        
        print("=" * 60)
        print(f"Summary: {total_inserted} inserted, {total_skipped} skipped")
        print("=" * 60)
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

