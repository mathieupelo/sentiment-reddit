# Unified Reddit Sentiment Analysis

## 🎯 **One Script for Everything**

This single script (`unified_sentiment.py`) handles your complete workflow:

1. ✅ **Checks MySQL** for existing Reddit posts
2. ✅ **Collects missing data** from Reddit API (only what's needed)
3. ✅ **Generates 700 days of sentiment signals** (each using last 30 days of posts)
4. ✅ **Exports CSV** for your backtesting software

## 🚀 **Usage**

### **Just Run It:**
```bash
python3.10 unified_sentiment.py
```

That's it! No arguments needed.

## 📊 **What It Does**

### **Smart Data Management**
- **Checks existing data** in MySQL database
- **Only collects missing data** from Reddit API
- **Reuses existing data** for fast subsequent runs

### **Sentiment Analysis**
- **700 days of signals** going back from today
- **No look-ahead bias**: Each day uses only the previous 30 days of posts
- **FinBERT + gaming keywords** for accurate sentiment
- **All 16 tickers**: EA, TTWO, NTES, RBLX, MSFT, SONY, WBD, NCBDY, GDEV, OTGLF, SNAL, GRVY, SQNXF, KSFTF, KNMCY, NEXOY

### **Fallback Handling**
- **No data = 0 sentiment** (no crashes)
- **Low confidence** when data is sparse
- **Continues processing** even if some tickers fail

## 📁 **Output**

### **CSV File**
Generated in `data/unified_sentiment_YYYY-MM-DD_YYYY-MM-DD.csv`

### **Format**
```csv
asof_date,ticker,signal_name,value,confidence,posts_analyzed,calculation_method,search_terms
2024-01-01,EA,SENTIMENT_RDDT,0.15,0.8,25,textblob_with_gaming_keywords,"ea, electronic arts, ea games"
2024-01-01,MSFT,SENTIMENT_RDDT,0.08,0.6,12,finbert_with_gaming_keywords,"msft, microsoft"
```

### **Columns**
- `asof_date`: Date for the sentiment signal
- `ticker`: Stock ticker symbol
- `signal_name`: Always "SENTIMENT_RDDT"
- `value`: Sentiment score (-1.0 to 1.0)
- `confidence`: Confidence level (0.0 to 1.0)
- `posts_analyzed`: Number of Reddit posts analyzed
- `calculation_method`: Method used (FinBERT or TextBlob)
- `search_terms`: Terms searched for this ticker

## ⚙️ **Configuration**

### **Environment Variables** (Required)
Create/update your `.env` file:
```env
# Reddit API (Required)
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=sentiment_reddit_bot/1.0

# MySQL (Required)
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=reddit_sentiment
```

## 📈 **Performance**

### **First Run** (No existing data)
- **Time**: 2-4 hours for 700 days
- **Data collected**: ~50,000-100,000 Reddit posts
- **Storage**: ~500MB-1GB in MySQL database

### **Subsequent Runs** (Existing data)
- **Time**: 10-30 minutes (only collects missing data)
- **Data collected**: Only new/missing posts
- **Storage**: Incremental updates

## 🎯 **Perfect for Your Backtesting**

### **Monthly Strategy**
1. Run: `python3.10 unified_sentiment.py`
2. Get CSV with 700 days of sentiment for all 16 tickers
3. Import into your backtesting software
4. Rank by sentiment to pick top 7 tickers
5. Compare performance vs buy & hold baseline

### **No Look-Ahead Bias**
- Each signal date only uses posts from the previous 30 days
- Perfect for historical backtesting
- No future information leaks into past signals

## 🔧 **Requirements**

### **Dependencies**
All installed for Python 3.10:
- torch, transformers (for FinBERT)
- pandas, numpy (for data processing)
- textblob (for sentiment analysis)
- praw (for Reddit API)
- mysql-connector-python (for database)

### **Database**
- MySQL server running
- Database `reddit_sentiment` created
- Tables created by the collector script

## 🚨 **Troubleshooting**

### **MySQL Connection Issues**
- Check your `.env` file MySQL settings
- Ensure MySQL server is running
- Verify database exists

### **Reddit API Issues**
- Check Reddit API credentials in `.env`
- Script will continue with fallback sentiment (0.0)

### **No Data Found**
- Normal for very old dates
- Script handles gracefully with 0 sentiment
- Check Reddit API credentials if recent dates have no data

## 🎉 **Benefits**

### **Before (Confusing)**
- ❌ Multiple scripts to run
- ❌ Manual data collection
- ❌ Separate analysis steps
- ❌ No data reuse

### **After (Unified)**
- ✅ **One command**: `python3.10 unified_sentiment.py`
- ✅ **Smart data management**: Only collects what's missing
- ✅ **Automatic workflow**: Everything in one step
- ✅ **Data reuse**: Fast subsequent runs
- ✅ **700 days of signals**: Ready for backtesting

**Perfect for your monthly sentiment-based trading strategy!**
