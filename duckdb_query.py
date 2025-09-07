import duckdb
import pandas as pd
import os
import glob
import logging

# Set up logging for better visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Connect to an in-memory DuckDB database
logger.info("Connecting to DuckDB...")
con = duckdb.connect()
logger.info("DuckDB connected.")

# Load Parquet extension
logger.info("Loading Parquet extension...")
con.execute("INSTALL parquet;")
con.execute("LOAD parquet;")

# Define the paths to the Parquet files
raw_data_path = './data/parquet/raw/*/*/*.parquet'
agg_1h_path = './data/parquet/agg_1h/*/*.parquet'
agg_24h_path = './data/parquet/agg_24h/*/*.parquet'

# Check if Parquet files exist before creating views
# This prevents errors if the Spark job hasn't written any data yet
if not glob.glob(raw_data_path):
    logger.warning("No raw Parquet files found. The Spark job may not be running or has not produced data yet.")
    # Exit gracefully if no data is available
    exit()

# Create views for the different data sources
# We use glob to read all Parquet files in the specified directories
logger.info("Creating views from Parquet files...")
con.execute(f"CREATE OR REPLACE VIEW raw_crypto AS SELECT * FROM read_parquet('{raw_data_path}')")
con.execute(f"CREATE OR REPLACE VIEW agg_1h AS SELECT * FROM read_parquet('{agg_1h_path}')")
con.execute(f"CREATE OR REPLACE VIEW agg_24h AS SELECT * FROM read_parquet('{agg_24h_path}')")

# --- Analytics Queries ---

# 1. Top 10 coins by market cap (24h)
# This query now uses the raw data view
logger.info("Executing: Top 10 coins by market cap (24h)")
try:
    top_10 = con.execute("""
        SELECT coin, AVG(market_cap) AS avg_market_cap
        FROM raw_crypto
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
        GROUP BY coin
        ORDER BY avg_market_cap DESC
        LIMIT 10
    """).fetchdf()
    print("Top 10 by Market Cap:\n", top_10)
except Exception as e:
    logger.error(f"Error executing Top 10 query: {e}")

# 2. Coins with >10% price change (1h)
# This query uses the raw data view, which contains the calculated pct_change
logger.info("Executing: Coins with >10% price change (1h)")
try:
    high_change = con.execute("""
        SELECT coin, MAX(pct_change) AS max_pct_change
        FROM raw_crypto
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
        GROUP BY coin
        HAVING max_pct_change > 0.10
    """).fetchdf()
    print("High Change Coins:\n", high_change)
except Exception as e:
    logger.error(f"Error executing high change query: {e}")

# 3. Sentiment-price correlation
# This query uses the raw data view
logger.info("Executing: Sentiment-price correlation")
try:
    correlation = con.execute("""
        SELECT corr(sentiment, pct_change) AS sentiment_price_corr
        FROM raw_crypto
    """).fetchdf()
    print("Sentiment-Price Correlation:\n", correlation)
except Exception as e:
    logger.error(f"Error executing correlation query: {e}")

# Note: Additional queries could join with the aggregated views, for example:
# SELECT t1.coin, t1.price, t2.rolling_avg_1h
# FROM raw_crypto t1
# JOIN agg_1h t2 ON t1.coin = t2.coin AND t1.timestamp BETWEEN t2.window_start_1h AND t2.window_end_1h
# WHERE ...

logger.info("Analytics complete.")
