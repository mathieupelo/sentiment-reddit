# 🚀 Quick Start Guide

## Run Your Analysis (One Command)

```bash
cd Reddit
python3.10 unified_sentiment_fixed.py
```

## What Happens

1. ✅ Connects to MySQL (localhost, user: root, password: 3421)
2. ✅ Checks for missing Reddit data
3. ✅ Collects missing posts from Reddit API
4. ✅ Generates 700 days of sentiment signals
5. ✅ Exports CSV to `data/` folder

## Output

**File**: `data/unified_sentiment_YYYY-MM-DD_YYYY-MM-DD.csv`

**Contains**: 11,200 sentiment signals (700 days × 16 tickers)

## Time

- **First run**: 2-4 hours (collecting all data)
- **Subsequent runs**: 10-30 minutes (reuses MySQL data)

## For Your Backtesting

1. Run the script
2. Wait for completion
3. Import CSV into your backtesting software
4. Rank by sentiment to pick top 7 tickers
5. Compare vs buy & hold baseline

## Tickers (16 Total)

EA, TTWO, NTES, RBLX, MSFT, SONY, WBD, NCBDY, GDEV, OTGLF, SNAL, GRVY, SQNXF, KSFTF, KNMCY, NEXOY

## Configuration

MySQL credentials are hardcoded in `unified_sentiment_fixed.py`:
- Host: localhost
- User: root  
- Password: 3421
- Database: reddit_sentiment

Reddit API credentials in `.env` file (auto-loaded)

## No Look-Ahead Bias

Each day's sentiment uses only the previous 30 days of posts. Perfect for historical backtesting.

---

**That's it! One command, everything handled automatically.** 🎉
