# Unified Sentiment Data Insertion Guide

This guide explains how to use the `insert_unified_sentiment.py` script to insert sentiment data into your database using the `ac-core` library.

## Prerequisites

1. **Install ac-core library** (if not already installed):
   ```bash
   pip install ac-core
   ```

2. **Ensure dependencies are installed**:
   ```bash
   cd sentiment-reddit/Reddit
   pip install -r requirements.txt
   ```

3. **Configure database credentials** in `.env` file:
   
   Location: `C:\Repository\AC\sentiment-reddit\Reddit\.env`
   
   ```bash
   # Option 1: Use DATABASE_URL (recommended for Supabase)
   DATABASE_URL=postgresql://postgres:your-password@your-host:5432/postgres?sslmode=require
   
   # Option 2: Use individual connection parameters
   DB_HOST=your-host
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=your-password
   DB_NAME=postgres
   ```

## Usage

### Basic Usage

```bash
cd C:\Repository\AC\sentiment-reddit

# Insert your CSV file
python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv
```

### Advanced Options

```bash
# Dry-run to validate data without inserting
python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv --dry-run

# Use custom batch size for insertion
python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv --batch-size 500

# Skip rows with missing or invalid data (by default, all rows are kept)
python insert_unified_sentiment.py data/unified_sentiment_2023-11-09_2025-10-09.csv --skip-invalid

# Get help and see all options
python insert_unified_sentiment.py --help
```

## What the Script Does

1. **Loads environment variables** from `Reddit/.env`
2. **Reads your CSV file** with columns:
   - `asof_date` (required)
   - `ticker` (required)
   - `signal_name` (required)
   - `value` (required)
   - `confidence`, `posts_analyzed`, `calculation_method`, `search_terms` (stored in metadata)
3. **Processes the data**:
   - Converts extra columns to metadata JSON
   - Validates required fields
   - **Keeps ALL rows by default** (warns about invalid rows but doesn't skip them)
   - Use `--skip-invalid` flag if you want to automatically remove problematic rows
4. **Inserts into database**:
   - Uses automatic upsert (replaces existing signals with same key)
   - Processes in batches for efficiency
   - Shows detailed progress and statistics

## Expected CSV Format

```csv
asof_date,ticker,signal_name,value,confidence,posts_analyzed,calculation_method,search_terms
2023-11-09,EA,SENTIMENT_RDDT,0.456,1.0,0,fallback_no_data,"ea, electronic arts"
2023-11-09,TTWO,SENTIMENT_RDDT,0.493,0.36,0,fallback_no_data,"ttwo, take-two"
```

The extra columns (`confidence`, `posts_analyzed`, `calculation_method`, `search_terms`) are automatically stored in the `metadata` JSON field in the database.

## Example Output

```
======================================================================
🚀 Unified Sentiment Data Insertion Script
======================================================================
✅ Loaded environment from: C:\Repository\AC\sentiment-reddit\Reddit\.env

📁 Reading CSV file: data/unified_sentiment_2023-11-09_2025-10-09.csv
   Total rows read: 11218
   Removed 1 empty rows
   Extra columns to store in metadata: ['confidence', 'posts_analyzed', 'calculation_method', 'search_terms']
   Final rows to insert: 11217

📊 Data Summary:
   Date range: 2023-11-09 to 2025-10-09
   Unique tickers: 15
   Unique signals: 1
   Signal names: ['SENTIMENT_RDDT']

🔌 Connecting to database...
✅ Database connection successful!

📤 Inserting data (batch size: 1000)...

======================================================================
📈 Insertion Results
======================================================================
✅ Status: SUCCESS
   Records processed: 11217
   Records inserted: 11217
   Duration: 15.42 seconds
   Insertion rate: 727 records/second

🔌 Database connection closed
======================================================================
```

## Troubleshooting

### Error: "ac-core library not found"
```bash
pip install ac-core
```

### Error: "Database credentials not found"
- Ensure your `.env` file exists at `C:\Repository\AC\sentiment-reddit\Reddit\.env`
- Verify the DATABASE_URL or DB_* variables are set correctly

### Error: "CSV file not found"
- Use the correct relative or absolute path to your CSV file
- If in doubt, use absolute path: `python insert_unified_sentiment.py C:\Repository\AC\sentiment-reddit\data\unified_sentiment_2023-11-09_2025-10-09.csv`

### Error: "Failed to connect to database"
- Check your database is running
- Verify the host, port, username, and password in `.env`
- For Supabase, ensure SSL mode is included in DATABASE_URL

## Database Schema

The data is inserted into the `signal_raw` table with this structure:

```sql
CREATE TABLE signal_raw (
    id SERIAL PRIMARY KEY,
    asof_date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    signal_name VARCHAR(100) NOT NULL,
    value FLOAT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asof_date, ticker, signal_name)
);
```

The metadata field will contain:
```json
{
  "confidence": 1.0,
  "posts_analyzed": 0,
  "calculation_method": "fallback_no_data",
  "search_terms": "ea, electronic arts, ea games"
}
```

## Notes

- The script uses **upsert** logic: if a signal already exists (same `asof_date`, `ticker`, `signal_name`), it will be replaced with the new value
- **By default, ALL rows are kept** and processed, even if they have issues
- The script will warn you about problematic rows but will attempt to insert them
- Use `--skip-invalid` flag if you want to automatically skip rows with missing data
- Progress and statistics are shown during insertion
- The database connection is automatically managed (opened and closed)
- **The original CSV file is never modified** - the script only reads it

## Support

For issues with:
- **The script**: Check this guide or the script's help: `python insert_unified_sentiment.py --help`
- **ac-core library**: See the [ac-core documentation](../ac-core/README.md)
- **Database connection**: Verify your `.env` file and database credentials

