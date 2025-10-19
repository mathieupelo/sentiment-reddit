#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Insert unified sentiment data into the database using ac-core library.

This script reads a CSV file with sentiment data and inserts it into the
signal_raw table using the ac-core library. Extra columns like confidence,
posts_analyzed, calculation_method, and search_terms are stored in metadata.

Usage:
    python insert_unified_sentiment.py <csv_file_path>

Example:
    python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

try:
    from ac_core import SignalInserter
except ImportError:
    print("[ERROR] ac-core library not found. Please install it first:")
    print("        pip install ac-core")
    sys.exit(1)


def load_environment():
    """Load environment variables from .env file."""
    # Try to load from Reddit/.env
    env_path = Path(__file__).parent / "Reddit" / ".env"
    
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[OK] Loaded environment from: {env_path}")
    else:
        print(f"[WARNING] .env file not found at {env_path}")
        print("          Attempting to use system environment variables...")
    
    # Check if required environment variables are set
    if not os.getenv('DATABASE_URL') and not os.getenv('DB_HOST'):
        print("\n[ERROR] Database credentials not found in environment.")
        print("\nPlease create a .env file at: C:\\Repository\\AC\\sentiment-reddit\\Reddit\\.env")
        print("\nWith the following content:")
        print("DATABASE_URL=postgresql://postgres:your-password@your-host:5432/postgres?sslmode=require")
        print("\nOr set individual variables:")
        print("DB_HOST=your-host")
        print("DB_PORT=5432")
        print("DB_USER=postgres")
        print("DB_PASSWORD=your-password")
        print("DB_NAME=postgres")
        sys.exit(1)


def prepare_dataframe(csv_path: Path, skip_invalid: bool = False) -> pd.DataFrame:
    """
    Read and prepare the CSV file for insertion.
    
    This function:
    1. Reads the CSV file
    2. Creates metadata field from extra columns
    3. Validates data
    4. Returns a DataFrame ready for insertion
    """
    print(f"\n[READING] CSV file: {csv_path}")
    
    # Read CSV file
    df = pd.read_csv(csv_path)
    print(f"          Total rows read: {len(df)}")
    
    # Check for required columns
    required_cols = ['asof_date', 'ticker', 'signal_name', 'value']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check for completely empty rows (all values are NaN)
    completely_empty = df[required_cols].isna().all(axis=1).sum()
    if completely_empty > 0:
        print(f"          [WARNING] Found {completely_empty} completely empty rows")
        if skip_invalid:
            df = df[~df[required_cols].isna().all(axis=1)]
            print(f"          Removed {completely_empty} completely empty rows")
    
    # Check for rows with missing required values
    rows_with_missing = df[required_cols].isna().any(axis=1).sum()
    if rows_with_missing > 0:
        print(f"          [WARNING] Found {rows_with_missing} rows with missing required values")
        if skip_invalid:
            df = df[~df[required_cols].isna().any(axis=1)]
            print(f"          Removed {rows_with_missing} rows with missing values")
        else:
            print(f"          These rows will be kept and processed (may cause insertion errors)")
    
    # Identify extra columns to store in metadata
    extra_cols = [col for col in df.columns if col not in required_cols]
    
    if extra_cols:
        print(f"          Extra columns to store in metadata: {extra_cols}")
        
        # Create metadata column from extra columns
        def create_metadata(row):
            metadata = {}
            for col in extra_cols:
                value = row[col]
                # Skip NaN values
                if pd.notna(value):
                    metadata[col] = value
            return metadata if metadata else None
        
        df['metadata'] = df.apply(create_metadata, axis=1)
        
        # Drop extra columns (they're now in metadata)
        df = df[required_cols + ['metadata']]
    
    # Convert asof_date to datetime if it's not already
    try:
        df['asof_date'] = pd.to_datetime(df['asof_date']).dt.date
    except Exception as e:
        print(f"          [WARNING] Some date values may be invalid: {e}")
        if not skip_invalid:
            print(f"          Continuing with all rows (may cause insertion errors)")
    
    print(f"          Final rows to process: {len(df)}")
    
    # Show data summary
    print(f"\n[SUMMARY] Data Summary:")
    if len(df) > 0 and df['asof_date'].notna().any():
        print(f"          Date range: {df['asof_date'].min()} to {df['asof_date'].max()}")
        print(f"          Unique tickers: {df['ticker'].nunique()}")
        print(f"          Unique signals: {df['signal_name'].nunique()}")
        print(f"          Signal names: {df['signal_name'].unique().tolist()}")
    else:
        print(f"          Unable to generate summary - check data validity")
    
    return df


def main():
    """Main function to insert sentiment data."""
    parser = argparse.ArgumentParser(
        description='Insert unified sentiment data into the database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv
  python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv --batch-size 500
        """
    )
    parser.add_argument(
        'csv_file',
        type=str,
        help='Path to the CSV file containing sentiment data'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records to insert per batch (default: 1000)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate data without inserting into database'
    )
    parser.add_argument(
        '--skip-invalid',
        action='store_true',
        help='Skip rows with missing or invalid data (default: keep all rows)'
    )
    
    args = parser.parse_args()
    
    # Convert to Path object
    csv_path = Path(args.csv_file)
    
    # Check if CSV file exists
    if not csv_path.exists():
        print(f"[ERROR] CSV file not found: {csv_path}")
        sys.exit(1)
    
    print("=" * 70)
    print("Unified Sentiment Data Insertion Script")
    print("=" * 70)
    
    # Load environment variables
    load_environment()
    
    try:
        # Prepare DataFrame
        df = prepare_dataframe(csv_path, skip_invalid=args.skip_invalid)
        
        if args.dry_run:
            print("\n[OK] Dry-run completed successfully!")
            print("     Data is valid and ready for insertion.")
            print("\n     To actually insert the data, run without --dry-run flag:")
            print(f"     python insert_unified_sentiment.py {args.csv_file}")
            return
        
        # Initialize SignalInserter
        print(f"\n[CONNECTING] Connecting to database...")
        inserter = SignalInserter()
        
        # Test connection
        if not inserter.test_connection():
            print("[ERROR] Failed to connect to database")
            print("        Please check your database credentials and connection")
            sys.exit(1)
        
        print("[OK] Database connection successful!")
        
        # Insert data
        print(f"\n[INSERTING] Inserting data (batch size: {args.batch_size})...")
        start_time = datetime.now()
        
        result = inserter.insert_from_dataframe(
            df,
            validate=True,
            batch_size=args.batch_size
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Display results
        print("\n" + "=" * 70)
        print("Insertion Results")
        print("=" * 70)
        
        if result['success']:
            print(f"[SUCCESS] Status: SUCCESS")
            print(f"          Records processed: {result['records_processed']}")
            print(f"          Records inserted: {result['records_inserted']}")
            print(f"          Duration: {duration:.2f} seconds")
            
            if result['records_processed'] > 0:
                rate = result['records_inserted'] / duration
                print(f"          Insertion rate: {rate:.0f} records/second")
            
            if result['warnings']:
                print(f"\n[WARNING] Warnings:")
                for warning in result['warnings']:
                    print(f"          - {warning}")
        else:
            print(f"[FAILED] Status: FAILED")
            print(f"         Records processed: {result['records_processed']}")
            print(f"         Records inserted: {result['records_inserted']}")
            
            if result['errors']:
                print(f"\n[ERROR] Errors:")
                for error in result['errors']:
                    print(f"        - {error}")
            
            if result['warnings']:
                print(f"\n[WARNING] Warnings:")
                for warning in result['warnings']:
                    print(f"          - {warning}")
        
        # Close connection
        inserter.close()
        print("\n[CLOSED] Database connection closed")
        print("=" * 70)
        
        # Exit with appropriate code
        sys.exit(0 if result['success'] else 1)
        
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

