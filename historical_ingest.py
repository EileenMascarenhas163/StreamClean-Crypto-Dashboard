import json
import pandas as pd
from kafka import KafkaProducer
import os

# Use kafka:9092 for Docker network
producer = KafkaProducer(bootstrap_servers='kafka:9092')

def ingest_file(file_path):
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.json'):
        df = pd.read_json(file_path, lines=True)
    else:
        return
    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send('crypto_prices', json.dumps(message).encode('utf-8'))
        print(f"Sent historical: {message}")

for file in os.listdir('./historical/'):
    ingest_file(f'./historical/{file}')