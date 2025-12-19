import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
from db_utils import get_connection, init_database
import time

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "crypto_raw_events"
KAFKA_GROUP_ID = "crypto_cleaner_group"

def create_kafka_consumer():
    """
    Creates and returns a Kafka consumer with retry logic.
    Retries up to 10 times with 5 second delays between attempts.
    
    Returns:
        KafkaConsumer: Connected Kafka consumer instance
    """
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=10000
            )
            print("Kafka consumer connected successfully")
            return consumer
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

def clean_and_store():
    """
    Reads cryptocurrency data from Kafka, cleans it using Pandas,
    and stores the cleaned data in SQLite database.
    
    Cleaning steps:
    1. Remove records with missing critical fields
    2. Fill missing numeric values with 0
    3. Convert data types and normalize strings
    4. Convert timestamps to datetime objects
    5. Remove duplicates
    6. Validate prices and ranks
    7. Store in SQLite events table
    """
    print("Starting crypto data cleaning job...")
    init_database()
    
    consumer = create_kafka_consumer()
    messages = []
    
    print("Consuming messages from Kafka...")
    for message in consumer:
        messages.append(message.value)
    
    consumer.close()
    
    if not messages:
        print("No new messages to process")
        return
    
    print(f"Consumed {len(messages)} messages from Kafka")
    
    # Convert to DataFrame
    df = pd.DataFrame(messages)
    
    print("Cleaning cryptocurrency data...")
    
    # 1. Remove records without essential fields
    df = df.dropna(subset=['id', 'symbol', 'name', 'current_price'])
    
    # 2. Fill missing numeric values with 0
    numeric_columns = ['market_cap', 'market_cap_rank', 'total_volume', 
                       'high_24h', 'low_24h', 'price_change_24h', 
                       'price_change_percentage_24h', 'circulating_supply', 
                       'total_supply', 'ath', 'atl']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 3. Ensure proper data types
    df['coin_id'] = df['id'].astype(str).str.strip().str.lower()
    df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
    df['name'] = df['name'].astype(str).str.strip()
    
    # 4. Convert timestamps to datetime
    df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    df['ath_date'] = pd.to_datetime(df['ath_date'], errors='coerce')
    df['atl_date'] = pd.to_datetime(df['atl_date'], errors='coerce')
    
    # 5. Remove duplicates (keep latest record for each coin)
    df = df.sort_values('last_updated', ascending=False)
    df = df.drop_duplicates(subset=['coin_id', 'last_updated'], keep='first')
    
    # 6. Filter out invalid prices (negative or extremely high)
    df = df[df['current_price'] > 0]
    df = df[df['current_price'] < 1e9]  # Remove unrealistic prices
    
    # 7. Ensure market cap rank is positive integer
    df['market_cap_rank'] = df['market_cap_rank'].fillna(0).astype(int)
    df = df[df['market_cap_rank'] >= 0]
    
    # 8. Select and rename columns for database
    df_clean = df[[
        'coin_id', 'symbol', 'name', 'current_price', 'market_cap',
        'market_cap_rank', 'total_volume', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_percentage_24h',
        'circulating_supply', 'total_supply', 'ath', 'ath_date',
        'atl', 'atl_date', 'last_updated'
    ]].copy()
    
    print(f"Cleaned data: {len(df_clean)} valid records")
    
    # Store in SQLite
    conn = get_connection()
    
    inserted = 0
    skipped = 0

    for _, row in df_clean.iterrows():
        try:
            cursor = conn.cursor()
            
            # Convert timestamps to ISO format strings
            last_updated_str = row['last_updated'].isoformat() if pd.notna(row['last_updated']) else None
            ath_date_str = row['ath_date'].isoformat() if pd.notna(row['ath_date']) else None
            atl_date_str = row['atl_date'].isoformat() if pd.notna(row['atl_date']) else None
            
            cursor.execute("""
                INSERT OR IGNORE INTO events 
                (coin_id, symbol, name, current_price, market_cap, market_cap_rank,
                 total_volume, high_24h, low_24h, price_change_24h, 
                 price_change_percentage_24h, circulating_supply, total_supply,
                 ath, ath_date, atl, atl_date, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['coin_id'],
                row['symbol'],
                row['name'],
                float(row['current_price']),
                float(row['market_cap']),
                int(row['market_cap_rank']),
                float(row['total_volume']),
                float(row['high_24h']),
                float(row['low_24h']),
                float(row['price_change_24h']),
                float(row['price_change_percentage_24h']),
                float(row['circulating_supply']),
                float(row['total_supply']),
                float(row['ath']),
                ath_date_str,
                float(row['atl']),
                atl_date_str,
                last_updated_str
            ))
            
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
                
        except Exception as e:
            print(f"Error inserting record for {row['coin_id']}: {e}")
            import traceback
            traceback.print_exc()
    
    conn.commit()
    conn.close()
    
    print(f"✓ INSERTED {inserted} new cryptocurrency records into database")
    print(f"✓ SKIPPED {skipped} duplicates")
    print("Cleaning job completed successfully")

if __name__ == "__main__":
    clean_and_store()