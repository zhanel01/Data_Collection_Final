import pandas as pd
from datetime import datetime, timedelta
from db_utils import get_connection, init_database

def compute_daily_analytics():
    """
    Computes daily analytics from cryptocurrency data and stores in summary table.
    
    Analytics computed:
    - Total records and unique coins
    - Price statistics (avg, max, min)
    - Total market capitalization
    - Average price change percentage
    - Top coin by market cap
    - Most volatile coin
    - Distribution of price increases vs decreases
    
    All computations use Pandas operations (no manual loops).
    """
    print("Starting daily cryptocurrency analytics job...")
    init_database()
    
    conn = get_connection()
    
    # Get yesterday's date
    yesterday = target_date = datetime.now().date()
    
    # Query all records from yesterday
    query = 'SELECT * FROM events WHERE DATE(last_updated) = ?'  
    df = pd.read_sql_query(query, conn, params=(yesterday,))
    
    if df.empty:
        print(f"No cryptocurrency data for {yesterday}")
        conn.close()
        return
    
    print(f"Analyzing {len(df)} cryptocurrency records for date: {yesterday}")
    
    # Compute analytics using pandas
    analytics = {}
    
    # Basic statistics
    analytics['summary_date'] = yesterday
    analytics['total_records'] = len(df)
    analytics['unique_coins'] = df['coin_id'].nunique()
    
    # Price statistics
    analytics['avg_price'] = df['current_price'].mean()
    analytics['max_price'] = df['current_price'].max()
    analytics['min_price'] = df['current_price'].min()
    
    # Market cap statistics
    analytics['total_market_cap'] = df['market_cap'].sum()
    
    # Price change statistics
    analytics['avg_price_change_24h'] = df['price_change_percentage_24h'].mean()
    
    # Top coin by market cap
    top_coin_idx = df['market_cap'].idxmax()
    analytics['top_coin_by_market_cap'] = df.loc[top_coin_idx, 'name']
    analytics['top_coin_market_cap'] = df.loc[top_coin_idx, 'market_cap']
    
    # Most volatile coin (highest absolute price change percentage)
    df['abs_price_change'] = df['price_change_percentage_24h'].abs()
    most_volatile_idx = df['abs_price_change'].idxmax()
    analytics['most_volatile_coin'] = df.loc[most_volatile_idx, 'name']
    analytics['most_volatile_change'] = df.loc[most_volatile_idx, 'price_change_percentage_24h']
    
    # Price change distribution
    analytics['coins_with_price_increase'] = len(df[df['price_change_percentage_24h'] > 0])
    analytics['coins_with_price_decrease'] = len(df[df['price_change_percentage_24h'] < 0])
    
    # Print summary
    print(f"\n=== Daily Analytics Summary for {yesterday} ===")
    print(f"Total records: {analytics['total_records']}")
    print(f"Unique coins: {analytics['unique_coins']}")
    print(f"Average price: ${analytics['avg_price']:.2f}")
    print(f"Total market cap: ${analytics['total_market_cap']:,.0f}")
    print(f"Average 24h price change: {analytics['avg_price_change_24h']:.2f}%")
    print(f"Top coin: {analytics['top_coin_by_market_cap']} (${analytics['top_coin_market_cap']:,.0f})")
    print(f"Most volatile: {analytics['most_volatile_coin']} ({analytics['most_volatile_change']:.2f}%)")
    print(f"Price increases: {analytics['coins_with_price_increase']}")
    print(f"Price decreases: {analytics['coins_with_price_decrease']}")
    
    # Store analytics in database
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO daily_summary 
        (summary_date, total_records, unique_coins, avg_price, max_price, min_price,
         total_market_cap, avg_price_change_24h, top_coin_by_market_cap, 
         top_coin_market_cap, most_volatile_coin, most_volatile_change,
         coins_with_price_increase, coins_with_price_decrease)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        analytics['summary_date'],
        analytics['total_records'],
        analytics['unique_coins'],
        analytics['avg_price'],
        analytics['max_price'],
        analytics['min_price'],
        analytics['total_market_cap'],
        analytics['avg_price_change_24h'],
        analytics['top_coin_by_market_cap'],
        analytics['top_coin_market_cap'],
        analytics['most_volatile_coin'],
        analytics['most_volatile_change'],
        analytics['coins_with_price_increase'],
        analytics['coins_with_price_decrease']
    ))
    
    conn.commit()
    conn.close()
    print("\n✓ Daily analytics stored successfully in daily_summary table")

if __name__ == '__main__':
    compute_daily_analytics()