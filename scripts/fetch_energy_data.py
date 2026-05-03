import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_energy_data():
    """Fetch Italian electricity demand data from ENTSO-E transparency platform"""
    
    # Using Open Power System Data - free, no API key needed
    url = "https://data.open-power-system-data.org/time_series/latest/time_series_60min_singleindex.csv"
    
    logger.info("Fetching Italian energy data...")
    
    # Download and filter Italy data
    df = pd.read_csv(url, 
                     usecols=['utc_timestamp', 'IT_load_actual_entsoe_transparency'],
                     parse_dates=['utc_timestamp'],
                     nrows=8760)  # Last ~1 year of hourly data
    
    df = df.dropna()
    df.columns = ['timestamp', 'load_mw']
    df['country'] = 'IT'
    df['fetched_at'] = datetime.utcnow()
    
    logger.info(f"Fetched {len(df)} records")
    return df

def save_to_postgres(df):
    """Save energy data to PostgreSQL"""
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    cur = conn.cursor()
    
    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ,
            load_mw FLOAT,
            country VARCHAR(5),
            fetched_at TIMESTAMPTZ
        )
    """)
    
    # Insert data
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO energy_data (timestamp, load_mw, country, fetched_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (row['timestamp'], row['load_mw'], row['country'], row['fetched_at']))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Data saved to PostgreSQL!")

if __name__ == "__main__":
    df = fetch_energy_data()
    save_to_postgres(df)