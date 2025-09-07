import requests
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
import ccxt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Kafka producer
producer = KafkaProducer(bootstrap_servers='kafka:9092')

# VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Fetch Reddit sentiment
def get_sentiment(coin):
    url = f'https://www.reddit.com/r/CryptoCurrency/search.json?q={coin}&restrict_sr=1&sort=new&limit=5'
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            posts = [
                post['data']['title'] + ' ' + post['data'].get('selftext', '')
                for post in data['data']['children']
            ]
            scores = [analyzer.polarity_scores(post)['compound'] for post in posts if posts]
            return sum(scores) / len(scores) if scores else 0.0
    except Exception as e:
        print(f"Reddit sentiment error for {coin}: {e}")
    return random.uniform(-1, 1)

# Fetch crypto data from CoinGecko + Binance
def get_crypto_data(coin):
    price_sources = []

    # CoinGecko
    try:
        cg_url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true"
        cg_data = requests.get(cg_url).json().get(coin, {})
        if cg_data and cg_data.get('usd', 0) > 0:
            price_sources.append(cg_data['usd'])
    except Exception as e:
        print(f"CoinGecko error for {coin}: {e}")
        cg_data = {} # Ensure cg_data is an empty dict if the request fails

    # Binance
    try:
        exchange = ccxt.binance()
        binance_symbol = f"{coin.upper()}/USDT"
        if binance_symbol in exchange.markets:
            binance_data = exchange.fetch_ticker(binance_symbol)
            if binance_data and binance_data.get('last', 0) > 0:
                price_sources.append(binance_data['last'])
    except Exception as e:
        print(f"Binance error for {coin}: {e}")

    # Final price (average of available sources)
    price = sum(price_sources) / len(price_sources) if price_sources else None

    if price is None:
        raise ValueError(f"No valid price found for {coin}")

    message = {
        'coin': coin,
        'price': price,
        'market_cap': cg_data.get('usd_market_cap', 0),
        'vol_24h': cg_data.get('usd_24h_vol', 0),
        'change_24h': cg_data.get('usd_24h_change', 0),
        'sentiment': get_sentiment(coin),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    # Simulate anomalies (optional)
    if random.random() < 0.1:
        message['price'] = -1.0 if random.random() < 0.5 else None

    return message

coins = ['bitcoin', 'ethereum']

while True:
    for coin in coins:
        try:
            message = get_crypto_data(coin)
            producer.send('crypto_prices', json.dumps(message).encode('utf-8'))
            print(f"Sent message for {coin}: {message}")
        except Exception as e:
            print(f"Could not get data for {coin}. Skipping this cycle.")
            print(e)
    time.sleep(5)  # Add a delay to avoid API rate limits
