# StreamClean Crypto Dashboard

A real-time cryptocurrency data pipeline and analytics dashboard built with Python, Kafka, Spark, DuckDB, and Streamlit. It ingests live and historical crypto data, processes it for cleaning and aggregation, stores it in Parquet format, and provides interactive visualizations and insights for financial crypto users.

## Overview

StreamClean fetches cryptocurrency market data (e.g., price, market cap, volume, sentiment) from public APIs like CoinGecko and Binance, along with Reddit sentiment. It uses Kafka for streaming, Spark for processing (cleaning, partitioning), DuckDB for fast SQL analytics, and Streamlit for an interactive dashboard. The pipeline is orchestrated with Prefect for scheduling.

Key benefits for crypto users:
- Monitor price trends, market cap changes, and sentiment correlations.
- Analyze historical data for backtesting trading strategies.
- Get actionable insights like trade alerts and volatility trends.

Key Components

Ingestion Layer (producer.py and historical_ingest.py):

Live Data: producer.py fetches real-time crypto data (price, market cap, volume, 24h change) from public APIs (CoinGecko, Binance via ccxt) and Reddit sentiment using VADER. It averages prices, simulates anomalies, and pushes JSON messages to Kafka's crypto_prices topic every 5 seconds.
Historical Backfill: historical_ingest.py reads CSV/JSON files from ./data/historical/ (e.g., sample.csv with columns like coin, price, timestamp) and pushes them to the same Kafka topic.
Technology: Kafka for streaming, requests/ccxt/vaderSentiment for data fetching.


Processing Layer (spark_job.py):

Spark Structured Streaming reads from Kafka (kafka:9092), parses JSON, cleans data (filters invalid prices, fills nulls), adds date from timestamp, and writes to Parquet in ./data/parquet/raw/, partitioned by date/coin.
Debugging: Console output for incoming data.
Technology: PySpark for distributed processing, with local master mode.


Storage Layer:

Raw Parquet files in ./data/parquet/raw/date=YYYY-MM-DD/coin=COIN/*.parquet.
Checkpointing in ./checkpoint/raw/ for fault tolerance.
Parquet is columnar, efficient for analytics (fast reads with DuckDB).


Analytics Layer (duckdb_query.py):

DuckDB queries Parquet files for insights: top coins by market cap, >10% price changes, sentiment-price correlation.
Views for raw/aggregated data, with logging for errors.
Technology: DuckDB for in-memory SQL querying.


Visualization Layer (dashboard.py and app.py):

Streamlit Dashboard: Interactive UI for filters (coins, date range), charts (price trends, volume bars, market cap heatmap, sentiment scatter), correlation, volatility, anomalies, alerts, and raw data display. Auto-refreshes every 60s.
Flask API (app.py): Additional backend for coins, market data, trading simulation (balance, portfolio, orders), Reddit posts. Supports sessions for user state.
Technology: Streamlit/Plotly for UI, Flask for API.


## Setup

1. **Clone the Repository**:
   ```
   git clone <repository-url>
   cd streamclean
   ```

2. **Create Directories**:
   ```
   mkdir data data/raw data/parquet data/historical checkpoint checkpoint/raw checkpoint/processed
   ```

3. **Add Historical Data (Optional)**:
   - Place CSV/JSON files in `./data/historical/` (e.g., `sample.csv`):
     ```csv
     coin,price,market_cap,vol_24h,change_24h,sentiment,timestamp
     bitcoin,50000,1000000000,50000000,-5.0,0.7,2025-08-14T10:00:00Z
     ethereum,2000,500000000,20000000,3.0,0.4,2025-08-14T10:00:00Z
     ```

4. **Build and Start Docker**:
   ```
   docker-compose build
   docker-compose up -d
   ```
   - Verifies: Zookeeper, Kafka, Streamlit (port 8501), Prefect (port 4200).

5. **Install Dependencies Locally (if testing outside Docker)**:
   ```
   pip install -r requirements.txt
   ```

## Running the Pipeline

1. **Ingest Historical Data** (Optional):
   ```
   docker exec -it streamclean-streamlit python historical_ingest.py
   ```

2. **Run Prefect Flow**:
   ```
   docker exec -it streamclean-prefect python prefect_flow.py
   ```
   - Schedules: Historical ingestion, producer, Spark processing, DuckDB analysis every 5 minutes.
   - Monitor Prefect UI: `http://localhost:4200`.

3. **Run DuckDB Analytics Manually** (Optional):
   ```
   docker exec -it streamclean-streamlit python duckdb_query.py
   ```
   - Outputs: Top coins, high changes, sentiment correlation.

4. **Access Dashboard**:
   - Open `http://localhost:8501`.
   - Select coins/date range, view charts/insights.

5. **Run Flask App (Optional, if using `app.py` for API)**:
   ```
   docker exec -it streamclean-streamlit python app.py
   ```
   - Access: `http://localhost:5000/api/market-data?coin=bitcoin`.

## Directory Structure

```
streamclean/
├── data/                       # Data storage
│   ├── raw/                    # Raw JSON files
│   ├── parquet/                # Processed Parquet files (raw/agg_1h/agg_24h)
│   └── historical/             # Historical CSV/JSON input files
├── checkpoint/                 # Spark checkpoints (raw/agg_1h/agg_24h)
├── Dockerfile                  # Docker image build
├── docker-compose.yml          # Docker stack configuration
├── producer.py                 # Live data ingestion
├── historical_ingest.py        # Historical data backfill
├── spark_job.py                # Spark processing
├── duckdb_query.py             # DuckDB analytics
├── dashboard.py                # Streamlit dashboard
├── app.py                      # Flask API (optional)
├── prefect_flow.py             # Prefect orchestration
├── requirements.txt            # Python dependencies
└── README.md                   # Documentation
