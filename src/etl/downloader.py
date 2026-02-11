import os
import argparse
from sodapy import Socrata
import pandas as pd
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DATASET_ID = "d6zx-ckhd"  # NYPD Calls for Service
DOMAIN = "data.cityofnewyork.us"
APP_TOKEN = os.getenv("APP_TOKEN") # Optional: User can provide APP_TOKEN in .env for higher limits
OUTPUT_DIR = "data/raw"

def download_data(start_year, end_year, limit=None):
    """Downloads data from NYC Open Data, chunked by year."""
    
    # Ensure raw data directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    client = Socrata(DOMAIN, APP_TOKEN)
    
    print(f"Starting download for years {start_year}-{end_year}...")
    
    for year in range(start_year, end_year + 1):
        output_file = os.path.join(OUTPUT_DIR, f"nypd_calls_{year}.csv")
        
        if os.path.exists(output_file):
            print(f"File {output_file} already exists. Skipping.")
            continue
            
        print(f"Downloading data for {year}...")
        
        # SoQL query to filter by date (assuming 'incident_date' or similar field exists)
        # Note: The actual column name needs to be verified. Common ones: 'created_date', 'dispatch_date'
        # Browsing the dataset, 'incident_date' seems correct or 'created_date'.
        # Let's try standard Socrata paging first without heavy filtering if possible, 
        # but for 20 years we NEED filtering.
        # Queries on 'd6zx-ckhd' usually use `incident_date`.
        
        # Construct simplified query for the year
        start_date = f"{year}-01-01T00:00:00"
        end_date = f"{year}-12-31T23:59:59"
        
        where_clause = f"incident_date >= '{start_date}' AND incident_date <= '{end_date}'"
        
        try:
            # Fetch using generator to handle large volume if supported or loop with offset
            # For simplicity in this script, we'll fetch in chunks of 50k and append
            
            chunk_size = 50000
            offset = 0
            file_mode = 'w'
            header = True
            total_records = 0
            
            while True:
                results = client.get(
                    DATASET_ID, 
                    where=where_clause,
                    limit=chunk_size,
                    offset=offset,
                    order="incident_date"
                )
                
                if not results:
                    break
                    
                df = pd.DataFrame.from_records(results)
                
                # Append to CSV
                df.to_csv(output_file, mode=file_mode, header=header, index=False)
                
                total_records += len(df)
                offset += chunk_size
                file_mode = 'a'
                header = False
                
                print(f"  Downloaded {total_records} rows for {year}...", end='\r')
                
                if limit and total_records >= limit:
                    break
            
            print(f"\nCompleted {year}: {total_records} rows saved to {output_file}")
            
        except Exception as e:
            print(f"Error downloading {year}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NYPD 911 Call Data")
    parser.add_argument("--start", type=int, default=2004, help="Start Year")
    parser.add_argument("--end", type=int, default=2024, help="End Year")
    parser.add_argument("--limit", type=int, help="Limit rows per year (for testing)")
    
    args = parser.parse_args()
    
    download_data(args.start, args.end, args.limit)
