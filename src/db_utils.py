import sqlite3
import os

DB_PATH = '/opt/airflow/data/crypto.db' 

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    return conn

def init_database():
    
    conn = get_connection()
    cursor = conn.cursor()
    
   
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT,
            symbol TEXT,
            name TEXT,
            current_price REAL,
            market_cap REAL,
            market_cap_rank INTEGER,
            total_volume REAL,
            high_24h REAL,
            low_24h REAL,
            price_change_24h REAL,
            price_change_percentage_24h REAL,
            circulating_supply REAL,
            total_supply REAL,
            ath REAL,
            ath_date TIMESTAMP,
            atl REAL,
            atl_date TIMESTAMP,
            last_updated TIMESTAMP,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(coin_id, last_updated)
        )
    ''')
    
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary_date DATE UNIQUE,
            total_records INTEGER,
            unique_coins INTEGER,
            avg_price REAL,
            max_price REAL,
            min_price REAL,
            total_market_cap REAL,
            avg_price_change_24h REAL,
            top_coin_by_market_cap TEXT,
            top_coin_market_cap REAL,
            most_volatile_coin TEXT,
            most_volatile_change REAL,
            coins_with_price_increase INTEGER,
            coins_with_price_decrease INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

if __name__ == '__main__':
    init_database()