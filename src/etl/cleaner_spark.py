import pandas as pd
import glob
import os
import argparse

def clean_data(input_dir, output_dir):
    """Cleans NYPD calls data using Pandas (Fallback for Spark)."""
    
    os.makedirs(output_dir, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    print(f"Found {len(csv_files)} files to clean with Pandas...")
    
    for file in csv_files:
        try:
            print(f"Processing {file}...")
            # specific columns to load to save memory
            df = pd.read_csv(file)
            
            # Rename columns to match schema
            rename_map = {
                "CAD_EVNT_ID": "cad_evnt_id",
                "CREATED_DATE": "created_date",
                "INCIDENT_DATE": "incident_date",
                "INCIDENT_TIME": "incident_time",
                "NY_CLI": "ny_cli",
                "ARRIVAL_TIME": "arrival_time",
                "CLOSING_TIME": "closing_time",
                "VOL_ID": "vol_id",
                "PRECINCT_ID": "precinct_id",
                "SECTOR_ID": "sector_id",
                "BORO_NM": "borough",
                "PATROL_BORO_NM": "patrol_boro",
                "TYP_DESC": "complaint_type",
                "CMPLNT_DESC": "descriptor",
                "City": "city",
                "Latitude": "latitude",
                "Longitude": "longitude"
            }
            # Handle potential mismatch in source columns vs case
            # make map case insensitive
            df.columns = [c.strip() for c in df.columns]
            
            # Apply renaming (only for columns that exist)
            df = df.rename(columns=rename_map)
            
            # Standardize columns
            # Ensure critical columns exist
            if 'incident_date' not in df.columns:
                print(f"Skipping {file}: Missing incident_date")
                continue
                
            # Date Parsing
            # Try flexible parsing
            for date_col in ['created_date', 'incident_date', 'arrival_time', 'closing_time']:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

            # Numeric conversion
            if 'precinct_id' in df.columns:
                df['precinct_id'] = pd.to_numeric(df['precinct_id'], errors='coerce')
                
            if 'latitude' in df.columns:
                df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                
            if 'longitude' in df.columns:
                df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

            # String standardization
            str_cols = ['borough', 'patrol_boro', 'complaint_type']
            for c in str_cols:
                if c in df.columns:
                    df[c] = df[c].astype(str).str.upper().str.strip()
                    
            # Deduplicate
            if 'cad_evnt_id' in df.columns:
                df = df.drop_duplicates(subset=['cad_evnt_id'])
            
            # Filter
            if 'incident_date' in df.columns:
                df = df.dropna(subset=['incident_date'])
            
            # Save to Parquet
            basename = os.path.basename(file).replace('.csv', '.parquet')
            out_path = os.path.join(output_dir, basename)
            
            print(f"Writing to {out_path}...")
            df.to_parquet(out_path, index=False)
            
        except Exception as e:
            print(f"Error processing {file}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    clean_data(args.input, args.output)
