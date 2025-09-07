from flask import Flask, jsonify, request, session
from flask_cors import CORS
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import uuid

app = Flask(__name__)
app.secret_key = 'crypto-trader-pro-secret'
CORS(app, resources={r"/api/*": {"origins": "*"}})
con = duckdb.connect()
con.execute("INSTALL parquet; LOAD parquet;")
analyzer = SentimentIntensityAnalyzer()
def query_duckdb(sql):
    try:
        return con.execute(sql).fetchdf(), None
    except Exception as e:
        return pd.DataFrame(), str(e)
def calculate_technical_indicators(data, ma_windows=[7, 20, 50]):
    if len(data) < 20:
        return data
    for window in ma_windows:
        data[f'ma_{window}'] = data['price'].rolling(window=window, min_periods=1).mean()
    delta = data['price'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    data['rsi'] = 100 - (100 / (1 + rs))
    data['bb_middle'] = data['price'].rolling(window=20).mean()
    bb_std = data['price'].rolling(window=20).std()
    data['bb_upper'] = data['bb_middle'] + (bb_std * 2)
    data['bb_lower'] = data['bb_middle'] - (bb_std * 2)
    exp1 = data['price'].ewm(span=12, adjust=False).mean()
    exp2 = data['price'].ewm(span=26, adjust=False).mean()
    data['macd'] = exp1 - exp2
    data['macd_signal'] = data['macd'].ewm(span=9, adjust=False).mean()
    data['macd_histogram'] = data['macd'] - data['macd_signal']
    return data
def get_session_state():
    if 'state' not in session:
        session['state'] = {
            'balance': 100000.0,
            'portfolio': {},
            'trade_history': [],
            'open_orders': [],
            'initial_balance': 100000.0,
        }
    return session['state']
@app.route('/api/coins', methods=['GET'])
def get_coins():
    coins, error = query_duckdb("SELECT DISTINCT coin FROM read_parquet('./data/parquet/raw/*/*/*.parquet')")
    if error:
        return jsonify({"error": "Failed to fetch coins"}), 500
    return jsonify(coins['coin'].tolist())
@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    coin = request.args.get('coin')
    timeframe = request.args.get('timeframe')
    filter_sql = f"WHERE coin = '{coin}'"
    data, error = query_duckdb(f"SELECT * FROM read_parquet('./data/parquet/raw/*/*/*.parquet') {filter_sql} ORDER BY timestamp ASC")
    if error:
        return jsonify({"error": f"Database query failed: {error}"}), 500
    if not data.empty:
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data = calculate_technical_indicators(data)
        data_json = data.to_dict('records')
        return jsonify(data_json), 200
    else:
        return jsonify({"message": "No data available for the selected coin and timeframe"}), 404
@app.route('/api/trade', methods=['POST'])
def trade():
    state = get_session_state()
    data = request.json
    trade_type = data.get('type')
    coin = data.get('coin')
    amount_usd = data.get('amount_usd')
    price = data.get('price')
    limit_price = data.get('limit_price')
    if trade_type == 'buy' and state['balance'] >= amount_usd:
        if price:
            coins_to_buy = amount_usd / price
            state['balance'] -= amount_usd
            state['portfolio'][coin] = state['portfolio'].get(coin, 0) + coins_to_buy
            state['trade_history'].append({
                'id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'coin': coin,
                'type': 'BUY',
                'amount': coins_to_buy,
                'price': price,
                'total_usd': amount_usd
            })
            session['state'] = state
            return jsonify({'message': 'Market buy successful'}), 200
        elif limit_price:
            state['open_orders'].append({
                'id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'coin': coin,
                'type': 'BUY_LIMIT',
                'amount': amount_usd / limit_price,
                'limit_price': limit_price,
                'total_usd': amount_usd
            })
            session['state'] = state
            return jsonify({'message': 'Limit buy order placed'}), 200
    elif trade_type == 'sell' and state['portfolio'].get(coin, 0) > 0:
        if price:
            coins_to_sell = min(amount_usd / price, state['portfolio'][coin])
            actual_usd = coins_to_sell * price
            state['balance'] += actual_usd
            state['portfolio'][coin] -= coins_to_sell
            state['trade_history'].append({
                'id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'coin': coin,
                'type': 'SELL',
                'amount': coins_to_sell,
                'price': price,
                'total_usd': actual_usd
            })
            session['state'] = state
            return jsonify({'message': 'Market sell successful'}), 200
    return jsonify({'error': 'Trade failed due to insufficient funds or holdings'}), 400
@app.route('/api/portfolio', methods=['GET'])
def get_portfolio():
    state = get_session_state()
    latest_prices = {}
    for coin in state['portfolio']:
        data, error = query_duckdb(f"SELECT price FROM read_parquet('./data/parquet/raw/*/*/*.parquet') WHERE coin = '{coin}' ORDER BY timestamp DESC LIMIT 1")
        if not error and not data.empty:
            latest_prices[coin] = data['price'].iloc[0]
    holdings_value = {coin: state['portfolio'][coin] * latest_prices.get(coin, 0) for coin in state['portfolio']}
    
    # Corrected: Convert holdings dictionary to a list of objects
    portfolio_list = []
    total_value = state['balance'] + sum(holdings_value.values())
    
    for coin, amount in state['portfolio'].items():
        value = holdings_value.get(coin, 0)
        allocation = (value / total_value * 100) if total_value > 0 else 0
        portfolio_list.append({
            'Coin': coin,
            'Amount': amount,
            'Value': value,
            'Allocation': allocation
        })
    
    total_pnl = total_value - state['initial_balance']
    total_pnl_pct = (total_pnl / state['initial_balance'] * 100) if state['initial_balance'] > 0 else 0
    
    return jsonify({
        'balance': state['balance'],
        'portfolio': portfolio_list,  # Use the new list
        'latest_prices': latest_prices,
        'holdings_value': holdings_value,
        'total_portfolio_value': total_value,
        'total_pnl': total_pnl,
        'total_pnl_pct': total_pnl_pct
    })
@app.route('/api/cancel-orders', methods=['POST'])
def cancel_orders():
    state = get_session_state()
    state['open_orders'] = []
    session['state'] = state
    return jsonify({'message': 'All orders cancelled'})
@app.route('/api/reddit-posts', methods=['GET'])
def get_reddit_posts():
    coin = request.args.get('coin', 'bitcoin')
    url = f'https://www.reddit.com/r/CryptoCurrency/search.json?q={coin}&restrict_sr=1&sort=new&limit=5'
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            posts = []
            for post in data['data']['children']:
                post_data = post['data']
                sentiment = analyzer.polarity_scores(post_data['title'] + ' ' + post_data.get('selftext', ''))['compound']
                posts.append({
                    'title': post_data['title'],
                    'text': post_data.get('selftext', ''),
                    'link': f"https://www.reddit.com{post_data['permalink']}",
                    'sentiment': sentiment,
                    'created': post_data['created_utc']
                })
            return jsonify(posts), 200
        else:
            return jsonify({'error': f"Reddit API returned status code {response.status_code}"}), 500
    except Exception as e:
        return jsonify({'error': f"Failed to fetch Reddit posts: {str(e)}"}), 500
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')