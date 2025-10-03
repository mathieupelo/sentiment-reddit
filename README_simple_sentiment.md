# Simple Reddit Sentiment Analysis

A straightforward Reddit sentiment analysis system that uses only ticker symbols and company names. Designed for integration with external backtesting software.

## Features

### 🎯 **Simple & Focused**
- **Only ticker + company name**: Searches for ticker symbols and company names
- **No complex keywords**: Avoids overwhelming keyword mapping
- **Point-in-time calculation**: Uses only data available at each specific date
- **No look-ahead bias**: Ensures historical accuracy for backtesting

### 🔍 **Straightforward Analysis**
- **Direct mentions**: Searches for ticker symbols (EA, MSFT, etc.)
- **Company names**: Searches for company names (electronic arts, microsoft, etc.)
- **Engagement weighting**: Weights sentiment by post score and comment count
- **Recency weighting**: Gives higher weight to more recent posts
- **Confidence scoring**: Provides confidence levels based on data quality

### 📊 **Backtesting Ready**
- **CSV export**: Clean format for external backtesting software
- **Point-in-time signals**: Each signal uses only data available up to that date
- **Metadata included**: Posts analyzed, confidence, search terms used
- **Flexible date ranges**: Generate signals for any historical period

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up Reddit API
Create a `.env` file with your Reddit API credentials:
```env
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=sentiment_reddit_bot/1.0
```

### 3. Generate Sentiment Signals
```bash
# Generate sentiment for last 30 days (default)
python simple_sentiment_main.py generate

# Generate sentiment for specific date range
python simple_sentiment_main.py generate --start-date 2024-01-01 --end-date 2024-01-31

# Generate sentiment for specific tickers
python simple_sentiment_main.py generate --tickers "EA,MSFT,SONY"

# Generate sentiment with custom lookback period
python simple_sentiment_main.py generate --lookback-days 60
```

## How It Works

### 1. **Simple Search Strategy**
- **Ticker Symbol**: Searches for exact ticker (EA, MSFT, SONY, etc.)
- **Company Name**: Searches for company name (electronic arts, microsoft, sony, etc.)
- **Reddit-wide search**: Searches across all of Reddit
- **Time filtering**: Only uses posts from the lookback period

### 2. **Point-in-Time Sentiment Calculation**
- For each date, uses only data available up to that date
- Searches for ticker and company name mentions
- Analyzes sentiment using TextBlob + gaming-specific keywords
- Weights by engagement and recency

### 3. **Simple Scoring**
- **Engagement weight**: Post score + (comments × 2)
- **Recency weight**: Higher weight for more recent posts
- **Confidence**: Based on number of posts found (max at 20+ posts)
- **Final score**: Weighted average of all sentiment scores

## Output Format

The system generates a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| `asof_date` | Date for the sentiment signal |
| `ticker` | Stock ticker symbol |
| `signal_name` | Always "SIMPLE_SENTIMENT_RDDT" |
| `value` | Sentiment score (-1.0 to 1.0) |
| `confidence` | Confidence level (0.0 to 1.0) |
| `posts_analyzed` | Number of Reddit posts analyzed |
| `calculation_method` | Method used for calculation |
| `search_terms` | Terms searched (ticker, company name) |

### Example Output
```csv
asof_date,ticker,signal_name,value,confidence,posts_analyzed,calculation_method,search_terms
2024-01-01,EA,SIMPLE_SENTIMENT_RDDT,0.15,0.8,25,simple_ticker_company,"ea, electronic arts"
2024-01-01,MSFT,SIMPLE_SENTIMENT_RDDT,0.08,0.6,12,simple_ticker_company,"msft, microsoft"
2024-01-01,RBLX,SIMPLE_SENTIMENT_RDDT,-0.05,0.4,8,simple_ticker_company,"rblx, roblox"
```

## Configuration Options

### Command Line Arguments
- `--start-date`: Start date for sentiment calculation (YYYY-MM-DD)
- `--end-date`: End date for sentiment calculation (YYYY-MM-DD)
- `--tickers`: Comma-separated list of tickers to analyze
- `--lookback-days`: Number of days to look back for sentiment (default: 30)
- `--output`: Output CSV file path

### Default Tickers
- EA, TTWO, NTES, RBLX, MSFT, SONY, WBD, NCBDY, GDEV, OTGLF, SNAL, GRVY

### Company Name Mapping
```python
{
    'EA': 'electronic arts',
    'MSFT': 'microsoft',
    'SONY': 'sony',
    'TTWO': 'take-two',
    'NTES': 'netease',
    'RBLX': 'roblox',
    'WBD': 'warner bros',
    'NCBDY': 'cd projekt',
    'GDEV': 'gaijin',
    'OTGLF': 'outlook',
    'SNAL': 'snail',
    'GRVY': 'gravity'
}
```

## Integration with Backtesting Software

### 1. **Generate Signals**
```bash
python simple_sentiment_main.py generate --start-date 2020-01-01 --end-date 2024-01-01
```

### 2. **Import to Backtesting Software**
- Load the generated CSV file
- Use `asof_date` as your date column
- Use `value` as your sentiment signal
- Use `confidence` to filter low-quality signals

### 3. **Example Backtesting Logic**
```python
# Load signals
signals = pd.read_csv('simple_sentiment_2020-01-01_2024-01-01.csv')

# Filter by confidence
high_confidence = signals[signals['confidence'] > 0.5]

# Create trading signals
trading_signals = high_confidence.copy()
trading_signals['signal'] = np.where(
    trading_signals['value'] > 0.1, 1,  # Buy
    np.where(trading_signals['value'] < -0.1, -1, 0)  # Sell
)

# Backtest with your existing software
```

## Advantages of Simple Approach

### ✅ **Easy to Understand**
- Clear search strategy (ticker + company name)
- No complex keyword mapping
- Straightforward sentiment calculation

### ✅ **Faster Processing**
- Fewer search terms per ticker
- Less API calls to Reddit
- Quicker sentiment calculation

### ✅ **Point-in-Time Accuracy**
- Uses only data available at each date
- No look-ahead bias
- Suitable for historical backtesting

### ✅ **Reliable Results**
- Focuses on direct mentions
- Less noise from irrelevant discussions
- Consistent search strategy

## Performance Considerations

### **Rate Limiting**
- Built-in rate limiting to respect Reddit API limits
- 0.5 second delay between search terms
- Efficient search strategies

### **Data Quality**
- Confidence scoring based on post count
- Engagement and recency weighting
- Simple but effective sentiment analysis

### **Scalability**
- Processes one ticker at a time
- Configurable lookback period
- Memory-efficient data structures

## Troubleshooting

### **No Data Found**
- Check Reddit API credentials
- Verify ticker symbols are correct
- Try increasing `--lookback-days`
- Check if ticker has sufficient Reddit discussion

### **Low Confidence Scores**
- Normal for less-discussed tickers
- Consider filtering by confidence level
- Increase lookback period for more data

### **API Rate Limits**
- System includes built-in rate limiting
- Reduce number of tickers if needed
- Check Reddit API status

## Support

For issues or questions:
1. Check the logs for error messages
2. Verify Reddit API credentials
3. Ensure sufficient Reddit discussion for your tickers
4. Try with a smaller date range first

## License

This project is part of the sentiment-reddit system for Reddit-based sentiment analysis.
