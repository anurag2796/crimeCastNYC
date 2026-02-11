import os
import psycopg2
import pandas as pd
import glob
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "nyc_911_calls"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}

import sqlite3

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception:
        print("PostgreSQL connection failed. Falling back to SQLite.")
        return sqlite3.connect("crimecast.db")

def load_parquet_to_postgres(processed_dir):
    """Loads Parquet files from processed directory into DB."""
    
    files = glob.glob(os.path.join(processed_dir, "*.parquet"))
    if not files:
        print(f"No parquet files found in {processed_dir}")
        return

    conn = get_connection()
    is_sqlite = isinstance(conn, sqlite3.Connection)
    
    if is_sqlite:
        print("Using SQLite for data loading...")
    else:
        print("Using PostgreSQL for data loading...")
        
    cursor = conn.cursor()
    
    # Create Table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS calls_for_service (
        cad_evnt_id TEXT,
        created_date DATE,
        incident_date DATE,
        incident_time TIME,
        ny_cli TEXT,
        arrival_time TIME,
        closing_time TIME,
        vol_id TEXT,
        precinct_id FLOAT,
        sector_id TEXT,
        borough TEXT,
        patrol_boro TEXT,
        complaint_type TEXT,
        descriptor TEXT,
        location_type_code TEXT,
        city TEXT,
        latitude FLOAT,
        longitude FLOAT
    );
    TRUNCATE TABLE calls_for_service;
    """
    if not is_sqlite:
        try:
            cursor.execute(create_table_query)
            conn.commit()
            print("Table 'calls_for_service' created/truncated.")
        except Exception as e:
            print(f"Error creating table: {e}")
            conn.rollback()

    try:
        for file in tqdm(files, desc="Loading Files"):
            df = pd.read_parquet(file)
            
            # Column mapping/filtering
            columns = [
                "cad_evnt_id", "created_date", "incident_date", "incident_time",
                "ny_cli", "arrival_time", "closing_time", "vol_id",
                "precinct_id", "sector_id", "borough", "patrol_boro",
                "complaint_type", "descriptor", "location_type_code",
                "city", "latitude", "longitude"
            ]
            
            # Align columns
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            df = df[columns]

            if is_sqlite:
                # SQLite loading via Pandas
                # Convert timestamps to string/ISO for SQLite
                for col in df.select_dtypes(include=['datetime64[ns]']):
                    df[col] = df[col].astype(str)
                    
                df.to_sql("calls_for_service", conn, if_exists='append', index=False)
            else:
                # Postgres COPY
                from io import StringIO
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
                buffer.seek(0)
                cursor.copy_from(buffer, 'calls_for_service', sep='\t', null='\\N', columns=columns)
                conn.commit()
                
        print("Data load complete.")
        
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/processed", help="Input directory containing parquet files")
    args = parser.parse_args()
    
    load_parquet_to_postgres(args.input)
