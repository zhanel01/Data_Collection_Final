import os
import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = 'kafka:29092'
KAFKA_TOPIC = 'crypto_raw_events'
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY', 'CG-tUjy8xDi8t81ceqaTCYUVmEC')
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3/coins/markets'

def create_kafka_producer():
    """
    Creates and returns a Kafka producer with retry logic.
    Retries up to 15 times with 10 second delays between attempts.
    """
    max_retries = 15
    retry_delay = 10

    print(f"Attempting to connect to Kafka at {KAFKA_BROKER}...")
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_block_ms=10000,
                request_timeout_ms=30000
            )
            print("Kafka producer connected successfully")
            return producer
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("FAILED TO CONNECT TO KAFKA")
                raise

def fetch_crypto_data():
    """
    Fetch cryptocurrency market data from CoinGecko API.
    Returns top 250 cryptocurrencies by market cap.
    
    Returns:
        list: List of cryptocurrency data dictionaries, or empty list on error
    """
    headers = {
        'x-cg-demo-api-key': COINGECKO_API_KEY
    }
    
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 250,
        'page': 1,
        'sparkline': False,
        'price_change_percentage': '24h'
    }
    
    try:
        response = requests.get(
            COINGECKO_API_URL, 
            headers=headers,
            params=params, 
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            print(f"Successfully fetched {len(data)} cryptocurrencies")
            return data
        else:
            print(f"API Error: Unexpected response format")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching crypto data: {e}")
        return []
    except Exception as e:
        print(f"Error fetching crypto data: {e}")
        return []

def produce_to_kafka(duration_minutes=2):
    """
    Continuously fetch crypto data and send to Kafka.
    Runs for specified duration in minutes.
    
    Args:
        duration_minutes (int): How long to run the continuous ingestion
    """
    producer = create_kafka_producer()
    start_time = time.time()
    iteration = 0
    total_sent = 0
    
    print(f"Starting continuous crypto data ingestion for {duration_minutes} minutes...")
    
    while time.time() - start_time < duration_minutes * 60:
        iteration += 1
        print(f"\n--- Iteration {iteration} at {datetime.now()} ---")
        
        crypto_data = fetch_crypto_data()
        
        if crypto_data:
            for coin in crypto_data:
                message = {
                    'id': coin.get('id'),
                    'symbol': coin.get('symbol'),
                    'name': coin.get('name'),
                    'image': coin.get('image'),
                    'current_price': coin.get('current_price'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
                    'total_volume': coin.get('total_volume'),
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'ath': coin.get('ath'),
                    'ath_change_percentage': coin.get('ath_change_percentage'),
                    'ath_date': coin.get('ath_date'),
                    'atl': coin.get('atl'),
                    'atl_change_percentage': coin.get('atl_change_percentage'),
                    'atl_date': coin.get('atl_date'),
                    'roi': coin.get('roi'),
                    'last_updated': coin.get('last_updated'),
                    'fetched_at': datetime.now().isoformat()
                }
                producer.send(KAFKA_TOPIC, value=message)
                total_sent += 1
            
            producer.flush()
            print(f"Sent {len(crypto_data)} cryptocurrency records to Kafka topic '{KAFKA_TOPIC}'")
            print(f"Total records sent: {total_sent}")
        else:
            print("No cryptocurrency data fetched")
        
        # Wait 2 minutes between fetches (CoinGecko updates frequently)
        print("Waiting 2 minutes before next fetch...")
        time.sleep(120)
    
    producer.close()
    print(f"\nProducer finished successfully. Total records sent: {total_sent}")

if __name__ == '__main__':
    produce_to_kafka()