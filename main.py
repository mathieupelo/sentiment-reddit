#!/usr/bin/env python3
"""
Main entry point for sentiment-reddit Reddit post fetching.

This script fetches Reddit posts for companies using keywords from the varrock schema
and stores them in the ORE database.

Usage:
    python main.py
    python main.py --date 2025-01-15
    python main.py --start-date 2025-01-01 --end-date 2025-01-31
    python main.py --date 2025-01-15 --tickers RBLX TTWO
    python main.py --date 2025-01-15 --universe "GameCore-8"
    python main.py --start-date 2025-01-01 --end-date 2025-01-07 --dry-run

Environment variables required:
    DATABASE_URL (main database for varrock schema)
    DATABASE_ORE_URL (ORE database for reddit_posts)
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from the sentiment-reddit directory
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    # Fallback to default .env loading
    load_dotenv()

from Reddit.src.database.fetch_company_posts import main

if __name__ == "__main__":
    sys.exit(main())

