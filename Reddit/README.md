# Reddit Sentiment Analysis for Gaming Stocks

## 🎯 Purpose
Generate 700 days of sentiment signals for 16 gaming company stocks using Reddit data, optimized for backtesting your top-7 monthly trading strategy.

## 🚀 How to Run

```bash
cd Reddit
python3.10 unified_sentiment_fixed.py
```

That's it! One command does everything.

## 📊 What It Does

1. **Connects to MySQL** - Uses existing data for fast processing
2. **Checks for missing data** - Only collects what's needed from Reddit API
3. **Generates 700 days of signals** - Each day uses last 30 days of posts (no look-ahead bias)
4. **Exports CSV** - Ready for your backtesting software

## 📈 Tickers Analyzed (16 Total)

- **EA** - Electronic Arts
- **TTWO** - Take-Two Interactive
- **NTES** - NetEase
- **RBLX** - Roblox
- **MSFT** - Microsoft
- **SONY** - Sony
- **WBD** - Warner Bros Discovery
- **NCBDY** - Bandai Namco
- **GDEV** - Gaijin Entertainment
- **OTGLF** - CD Projekt
- **SNAL** - Snail Games
- **GRVY** - Gravity Co.
- **SQNXF** - Square Enix *(new)*
- **KSFTF** - Kingsoft *(new)*
- **KNMCY** - KONAMI *(new)*
- **NEXOY** - NEXON *(new)*

## 📁 Output

**File**: `data/unified_sentiment_YYYY-MM-DD_YYYY-MM-DD.csv`

**Columns**:
- `asof_date` - Date for the sentiment signal
- `ticker` - Stock ticker symbol
- `signal_name` - Always "SENTIMENT_RDDT"
- `value` - Sentiment score (-1.0 to 1.0)
- `confidence` - Confidence level (0.0 to 1.0)
- `posts_analyzed` - Number of Reddit posts analyzed
- `calculation_method` - Method used (FinBERT or TextBlob)
- `search_terms` - Terms searched for this ticker

## ⚙️ Configuration

**Environment Variables** (`.env` file):
```env
# Reddit API
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=sentiment_bot/1.0

# MySQL (hardcoded in script for now)
# host: localhost
# user: root
# password: 3421
# database: reddit_sentiment
```

## 📈 Performance

### First Run (No existing data)
- **Time**: 2-4 hours
- **Data collected**: ~50,000-100,000 Reddit posts
- **Storage**: ~500MB-1GB in MySQL

### Subsequent Runs (Existing data)
- **Time**: 10-30 minutes
- **Data collected**: Only new/missing posts
- **Storage**: Incremental updates

## 🎯 For Your Backtesting

1. Run: `python3.10 unified_sentiment_fixed.py`
2. Get CSV with 700 days × 16 tickers = 11,200 sentiment signals
3. Import into your backtesting software
4. Rank by sentiment to pick top 7 tickers each month
5. Compare performance vs buy & hold baseline

## 🔧 Key Features

- ✅ **No look-ahead bias** - Each signal uses only past data
- ✅ **MySQL data reuse** - Fast subsequent runs
- ✅ **FinBERT sentiment** - Superior financial sentiment analysis
- ✅ **Gaming-specific keywords** - Enhanced accuracy for gaming stocks
- ✅ **Fallback handling** - Returns 0 sentiment if no data found
- ✅ **Point-in-time calculation** - Perfect for historical backtesting

## 📂 Project Structure

```
Reddit/
├── unified_sentiment_fixed.py    # Main script (run this!)
├── mysql_sentiment_calculator.py # MySQL sentiment calculation
├── reddit_mysql_collector.py     # Reddit data collection
├── requirements.txt              # Python dependencies
├── .env                          # Configuration (not in git)
├── data/                         # Output CSV files
└── src/
    └── utils/
        └── calculate_signals.py  # Core sentiment logic
```

## 🚨 Important Files

### **Keep These:**
- `unified_sentiment_fixed.py` - Main script
- `mysql_sentiment_calculator.py` - MySQL calculator
- `reddit_mysql_collector.py` - Reddit collector
- `src/utils/calculate_signals.py` - Core logic
- `requirements.txt` - Dependencies
- `.env` - Configuration
- `README_unified_simple.md` - Detailed documentation

### **Already Deleted (Useless):**
- ~~`main.py`~~ - Old script
- ~~`simple_sentiment_main.py`~~ - Old script
- ~~`test_*.py`~~ - Test scripts
- ~~`unified_sentiment.py`~~ - Old versions
- ~~Old README files~~ - Outdated docs

## 💡 Tips

1. **First run takes time** - Be patient, it's collecting 700 days of data
2. **Subsequent runs are fast** - MySQL reuses existing data
3. **Check output CSV** - Verify signals before backtesting
4. **Monitor posts_analyzed** - Higher is better for confidence
5. **Run monthly** - Keep data fresh for your strategy

---

**Your unified sentiment analysis system is ready! 🚀**
