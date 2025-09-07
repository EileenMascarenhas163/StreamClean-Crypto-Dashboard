import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import os
import glob
from datetime import datetime, timedelta
import numpy as np


# Set Streamlit page config with dark theme
st.set_page_config(
    page_title="CryptoTrader Pro",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for crypto trading theme
st.markdown("""
<style>
    .main {
        background: linear-gradient(135deg, #0c0c0c 0%, #1a1a2e 100%);
    }
    .stMetric {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        padding: 1rem;
        backdrop-filter: blur(10px);
    }
    .profit { color: #00ff88 !important; }
    .loss { color: #ff4757 !important; }
    .neutral { color: #ffa502 !important; }
    .trading-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
    }
    .price-ticker {
        font-family: 'Courier New', monospace;
        font-size: 2rem;
        font-weight: bold;
        text-align: center;
        padding: 1rem;
        border-radius: 10px;
        background: rgba(0, 255, 136, 0.1);
        border: 2px solid #00ff88;
    }
</style>
""", unsafe_allow_html=True)

# --- Initialize DuckDB connection ---
@st.cache_resource
def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL parquet; LOAD parquet;")
    return con

con = init_duckdb()

# --- Enhanced helper functions ---
@st.cache_data(ttl=30)
def query_duckdb(sql):
    try:
        return con.execute(sql).fetchdf()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def calculate_technical_indicators(data):
    """Calculate various technical indicators"""
    if len(data) < 20:
        return data
    
    # Moving averages
    data['ma_7'] = data['price'].rolling(window=7, min_periods=1).mean()
    data['ma_20'] = data['price'].rolling(window=20, min_periods=1).mean()
    data['ma_50'] = data['price'].rolling(window=50, min_periods=1).mean()
    
    # RSI
    delta = data['price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    data['rsi'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands
    data['bb_middle'] = data['price'].rolling(window=20).mean()
    bb_std = data['price'].rolling(window=20).std()
    data['bb_upper'] = data['bb_middle'] + (bb_std * 2)
    data['bb_lower'] = data['bb_middle'] - (bb_std * 2)
    
    # MACD
    exp1 = data['price'].ewm(span=12).mean()
    exp2 = data['price'].ewm(span=26).mean()
    data['macd'] = exp1 - exp2
    data['macd_signal'] = data['macd'].ewm(span=9).mean()
    data['macd_histogram'] = data['macd'] - data['macd_signal']
    
    return data

def format_currency(value):
    """Format currency with appropriate color"""
    if value > 0:
        return f'<span class="profit">+${value:,.2f}</span>'
    elif value < 0:
        return f'<span class="loss">${value:,.2f}</span>'
    else:
        return f'<span class="neutral">${value:,.2f}</span>'

# --- Initialize Session State ---
if 'balance' not in st.session_state:
    st.session_state.balance = 100000.0  # Start with $100k
if 'portfolio' not in st.session_state:
    st.session_state.portfolio = {}
if 'trade_history' not in st.session_state:
    st.session_state.trade_history = []
if 'open_orders' not in st.session_state:
    st.session_state.open_orders = []
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = ['bitcoin', 'ethereum']
if 'initial_balance' not in st.session_state:
    st.session_state.initial_balance = 100000.0

# --- Header ---
st.markdown("# ₿ CryptoTrader Pro")
st.markdown("### Professional Paper Trading Platform")

# --- Sidebar Controls ---
st.sidebar.markdown("## 🎛️ Trading Controls")

# Market selector
try:
    coins = query_duckdb("SELECT DISTINCT coin FROM read_parquet('./data/parquet/raw/*/*/*.parquet')")['coin'].tolist()
except:
    coins = ['bitcoin', 'ethereum', 'cardano', 'solana', 'chainlink']

selected_coin = st.sidebar.selectbox("📈 Select Market", coins, key='coin_select')

# Timeframe selector
timeframe = st.sidebar.selectbox(
    "⏰ Timeframe", 
    ['1H', '4H', '1D', '1W', '1M'],
    index=2
)

# Technical indicators
st.sidebar.markdown("### 📊 Technical Indicators")
show_ma = st.sidebar.checkbox("Moving Averages", value=True)
show_bb = st.sidebar.checkbox("Bollinger Bands", value=False)
show_rsi = st.sidebar.checkbox("RSI", value=False)
show_macd = st.sidebar.checkbox("MACD", value=False)
show_volume = st.sidebar.checkbox("Volume", value=True)

# Trading mode
st.sidebar.markdown("### 🎯 Trading Mode")
trading_mode = st.sidebar.radio(
    "Select Mode",
    ["Market Order", "Limit Order", "Stop Loss"]
)

# --- Main Dashboard ---
col1, col2, col3, col4 = st.columns(4)

# Load data
filter_sql = f"WHERE coin = '{selected_coin}'"
data = query_duckdb(f"SELECT * FROM read_parquet('./data/parquet/raw/*/*/*.parquet') {filter_sql} ORDER BY timestamp ASC")

if not data.empty:
    data = calculate_technical_indicators(data)
    latest_price = data['price'].iloc[-1]
    price_change = data['price'].iloc[-1] - data['price'].iloc[-2] if len(data) > 1 else 0
    price_change_pct = (price_change / data['price'].iloc[-2] * 100) if len(data) > 1 else 0
    
    # Portfolio metrics
    holdings_value = st.session_state.portfolio.get(selected_coin, 0) * latest_price
    total_portfolio_value = st.session_state.balance + sum(
        st.session_state.portfolio.get(coin, 0) * latest_price for coin in st.session_state.portfolio
    )
    total_pnl = total_portfolio_value - st.session_state.initial_balance
    total_pnl_pct = (total_pnl / st.session_state.initial_balance * 100)
    
    with col1:
        st.metric(
            f"{selected_coin.upper()} Price",
            f"${latest_price:,.2f}",
            f"{price_change_pct:+.2f}%"
        )
    
    with col2:
        st.metric(
            "Portfolio Value",
            f"${total_portfolio_value:,.2f}",
            f"{total_pnl_pct:+.2f}%"
        )
    
    with col3:
        st.metric(
            "Available Balance",
            f"${st.session_state.balance:,.2f}"
        )
    
    with col4:
        st.metric(
            f"{selected_coin.upper()} Holdings",
            f"{st.session_state.portfolio.get(selected_coin, 0):.6f}",
            f"${holdings_value:,.2f}"
        )

    # --- Advanced Chart ---
    st.markdown("## 📈 Advanced Trading Chart")
    
    # Create subplots
    rows = 1
    if show_rsi: rows += 1
    if show_macd: rows += 1
    if show_volume: rows += 1
    
    subplot_titles = ["Price Chart"]
    if show_volume: subplot_titles.append("Volume")
    if show_rsi: subplot_titles.append("RSI")
    if show_macd: subplot_titles.append("MACD")
    
    fig = make_subplots(
        rows=rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=subplot_titles,
        row_heights=[0.6] + [0.4/(rows-1)]*(rows-1) if rows > 1 else [1]
    )
    
    # Main price chart (Candlestick would be ideal, but using line for now)
    fig.add_trace(
        go.Scatter(
            x=data['timestamp'],
            y=data['price'],
            mode='lines',
            name=f'{selected_coin.upper()} Price',
            line=dict(color='#00ff88', width=2)
        ),
        row=1, col=1
    )
    
    # Moving averages
    if show_ma:
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['ma_7'],
                mode='lines',
                name='MA 7',
                line=dict(color='#ffa502', width=1)
            ),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['ma_20'],
                mode='lines',
                name='MA 20',
                line=dict(color='#ff6b6b', width=1)
            ),
            row=1, col=1
        )
    
    # Bollinger Bands
    if show_bb:
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['bb_upper'],
                mode='lines',
                name='BB Upper',
                line=dict(color='rgba(255, 255, 255, 0.3)', width=1),
                fill=None
            ),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['bb_lower'],
                mode='lines',
                name='BB Lower',
                line=dict(color='rgba(255, 255, 255, 0.3)', width=1),
                fill='tonexty',
                fillcolor='rgba(255, 255, 255, 0.1)'
            ),
            row=1, col=1
        )
    
    current_row = 1
    
    # Volume
    if show_volume:
        current_row += 1
        if 'vol_24h' in data.columns:
            fig.add_trace(
                go.Bar(
                    x=data['timestamp'],
                    y=data['vol_24h'],
                    name='Volume',
                    marker_color='rgba(0, 255, 136, 0.3)'
                ),
                row=current_row, col=1
            )
    
    # RSI
    if show_rsi:
        current_row += 1
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['rsi'],
                mode='lines',
                name='RSI',
                line=dict(color='#ff4757', width=2)
            ),
            row=current_row, col=1
        )
        # RSI levels
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=current_row, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=current_row, col=1)
    
    # MACD
    if show_macd:
        current_row += 1
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['macd'],
                mode='lines',
                name='MACD',
                line=dict(color='#3742fa', width=2)
            ),
            row=current_row, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=data['timestamp'],
                y=data['macd_signal'],
                mode='lines',
                name='Signal',
                line=dict(color='#ff6b6b', width=1)
            ),
            row=current_row, col=1
        )
        fig.add_trace(
            go.Bar(
                x=data['timestamp'],
                y=data['macd_histogram'],
                name='Histogram',
                marker_color='rgba(255, 255, 255, 0.3)'
            ),
            row=current_row, col=1
        )
    
    fig.update_layout(
        height=600,
        template='plotly_dark',
        showlegend=True,
        xaxis_rangeslider_visible=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # --- Trading Panel ---
    st.markdown("## 💰 Trading Panel")
    
    trade_col1, trade_col2 = st.columns([2, 1])
    
    with trade_col1:
        st.markdown('<div class="trading-card">', unsafe_allow_html=True)
        
        # Trade amount input
        trade_amount = st.number_input(
            "Trade Amount (USD)",
            min_value=1.0,
            value=1000.0,
            step=100.0,
            key='trade_amount'
        )
        
        if trading_mode == "Limit Order":
            limit_price = st.number_input(
                "Limit Price",
                min_value=0.01,
                value=latest_price,
                step=0.01,
                key='limit_price'
            )
        elif trading_mode == "Stop Loss":
            stop_price = st.number_input(
                "Stop Price",
                min_value=0.01,
                value=latest_price * 0.95,
                step=0.01,
                key='stop_price'
            )
        
        # Buy/Sell buttons
        buy_col, sell_col = st.columns(2)
        
        with buy_col:
            if st.button(f"🟢 BUY {selected_coin.upper()}", key='buy_button', use_container_width=True):
                if trading_mode == "Market Order":
                    if st.session_state.balance >= trade_amount:
                        coins_to_buy = trade_amount / latest_price
                        st.session_state.balance -= trade_amount
                        st.session_state.portfolio[selected_coin] = st.session_state.portfolio.get(selected_coin, 0) + coins_to_buy
                        
                        # Add to trade history
                        st.session_state.trade_history.append({
                            'timestamp': datetime.now(),
                            'coin': selected_coin,
                            'type': 'BUY',
                            'amount': coins_to_buy,
                            'price': latest_price,
                            'total': trade_amount
                        })
                        
                        st.success(f"✅ Bought {coins_to_buy:.6f} {selected_coin.upper()} for ${trade_amount:,.2f}")
                    else:
                        st.error("❌ Insufficient balance")
                
                elif trading_mode == "Limit Order":
                    # Add to open orders
                    st.session_state.open_orders.append({
                        'timestamp': datetime.now(),
                        'coin': selected_coin,
                        'type': 'BUY_LIMIT',
                        'amount': trade_amount / limit_price,
                        'price': limit_price,
                        'total': trade_amount,
                        'status': 'PENDING'
                    })
                    st.info(f"📋 Limit buy order placed for {selected_coin.upper()} at ${limit_price:.2f}")
        
        with sell_col:
            if st.button(f"🔴 SELL {selected_coin.upper()}", key='sell_button', use_container_width=True):
                current_holdings = st.session_state.portfolio.get(selected_coin, 0)
                if trading_mode == "Market Order":
                    coins_to_sell = min(trade_amount / latest_price, current_holdings)
                    if coins_to_sell > 0:
                        actual_amount = coins_to_sell * latest_price
                        st.session_state.balance += actual_amount
                        st.session_state.portfolio[selected_coin] -= coins_to_sell
                        
                        # Add to trade history
                        st.session_state.trade_history.append({
                            'timestamp': datetime.now(),
                            'coin': selected_coin,
                            'type': 'SELL',
                            'amount': coins_to_sell,
                            'price': latest_price,
                            'total': actual_amount
                        })
                        
                        st.success(f"✅ Sold {coins_to_sell:.6f} {selected_coin.upper()} for ${actual_amount:,.2f}")
                    else:
                        st.error("❌ Insufficient holdings")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with trade_col2:
        # Market info
        st.markdown("### 📊 Market Info")
        
        if len(data) > 1:
            high_24h = data['price'].tail(24).max() if len(data) >= 24 else data['price'].max()
            low_24h = data['price'].tail(24).min() if len(data) >= 24 else data['price'].min()
            
            st.metric("24h High", f"${high_24h:,.2f}")
            st.metric("24h Low", f"${low_24h:,.2f}")
            
            if 'rsi' in data.columns:
                current_rsi = data['rsi'].iloc[-1]
                rsi_signal = "Overbought" if current_rsi > 70 else "Oversold" if current_rsi < 30 else "Neutral"
                st.metric("RSI", f"{current_rsi:.1f}", rsi_signal)

    # --- Portfolio Overview ---
    st.markdown("## 📊 Portfolio Overview")
    
    port_col1, port_col2 = st.columns(2)
    
    with port_col1:
        st.markdown("### Holdings")
        if st.session_state.portfolio:
            portfolio_df = pd.DataFrame([
                {
                    'Coin': coin.upper(),
                    'Amount': amount,
                    'Value': f"${amount * latest_price:,.2f}",
                    'Allocation': f"{(amount * latest_price / total_portfolio_value * 100):.1f}%"
                }
                for coin, amount in st.session_state.portfolio.items() if amount > 0
            ])
            st.dataframe(portfolio_df, use_container_width=True)
        else:
            st.info("No holdings yet. Start trading to build your portfolio!")
    
    with port_col2:
        st.markdown("### Recent Trades")
        if st.session_state.trade_history:
            recent_trades = pd.DataFrame(st.session_state.trade_history[-10:])
            recent_trades['timestamp'] = recent_trades['timestamp'].dt.strftime('%H:%M:%S')
            st.dataframe(recent_trades[['timestamp', 'coin', 'type', 'amount', 'price']], use_container_width=True)
        else:
            st.info("No trades yet. Execute your first trade above!")

    # --- Open Orders ---
    if st.session_state.open_orders:
        st.markdown("### 📋 Open Orders")
        orders_df = pd.DataFrame(st.session_state.open_orders)
        orders_df['timestamp'] = orders_df['timestamp'].dt.strftime('%H:%M:%S')
        st.dataframe(orders_df, use_container_width=True)
        
        if st.button("Cancel All Orders"):
            st.session_state.open_orders = []
            st.success("All orders cancelled")

    # --- Performance Analytics ---
    st.markdown("## 📈 Performance Analytics")
    
    perf_col1, perf_col2, perf_col3 = st.columns(3)
    
    with perf_col1:
        st.markdown(f"**Total P&L:** {format_currency(total_pnl)}", unsafe_allow_html=True)
    
    with perf_col2:
        st.markdown(f"**Return:** {total_pnl_pct:+.2f}%", unsafe_allow_html=True)
    
    with perf_col3:
        trades_count = len(st.session_state.trade_history)
        st.markdown(f"**Total Trades:** {trades_count}")

else:
    st.warning(f"⚠️ No data available for {selected_coin}. Please check your data source.")
# --- Reddit Sentiment & Trends ---
st.markdown("## 📰 Reddit Sentiment & Trends")



api_url = "http://localhost:5000/api/reddit-posts"  # Adjust if Flask runs elsewhere
try:
    response = requests.get(api_url, params={"coin": selected_coin}, timeout=10)
    if response.status_code == 200:
        reddit_posts = response.json()
        if reddit_posts:
            # Build DataFrame
            reddit_df = pd.DataFrame(reddit_posts)
            reddit_df['created'] = pd.to_datetime(reddit_df['created'], unit='s')
            
            # Sentiment coloring
            def sentiment_label(score):
                if score > 0.2: return "🟢 Positive"
                elif score < -0.2: return "🔴 Negative"
                else: return "🟡 Neutral"
            reddit_df['Sentiment'] = reddit_df['sentiment'].apply(sentiment_label)
            
            # Display posts
            for _, row in reddit_df.iterrows():
                st.markdown(f"**[{row['title']}]({row['link']})**")
                st.caption(f"Sentiment: {row['Sentiment']} | {row['created']}")
                if row['text']:
                    st.write(row['text'][:200] + "..." if len(row['text']) > 200 else row['text'])
                st.markdown("---")
            
            # Sentiment trend chart
            fig_sent = px.line(
                reddit_df.sort_values("created"),
                x="created", y="sentiment",
                markers=True, title=f"Reddit Sentiment Trend for {selected_coin.upper()}",
            )
            fig_sent.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig_sent, use_container_width=True)
        else:
            st.info("No recent Reddit posts found.")
    else:
        st.error(f"Failed to fetch posts: {response.status_code}")
except Exception as e:
    st.error(f"Error fetching Reddit posts: {e}")

# --- Auto-refresh ---
if st.sidebar.checkbox("🔄 Auto Refresh", value=True):
    time.sleep(5)
    st.rerun()
