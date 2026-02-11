import argparse
import logging
from src.etl import downloader, cleaner, loader
from src.analysis import ml, mining
from src.visualization import generator
import pandas as pd
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline(args):
    """Orchestrates the data pipeline."""
    
    # 1. Download
    if args.step in ['all', 'download']:
        logger.info("Starting Download Step...")
        downloader.download_data(args.start_year, args.end_year, args.limit)
    
    # 2. Clean
    if args.step in ['all', 'clean']:
        logger.info("Starting Clean Step...")
        raw_path = "data/raw"
        processed_path = "data/processed"
        cleaner.clean_data(raw_path, processed_path)
        
    # 3. Load
    if args.step in ['all', 'load']:
        logger.info("Starting Load Step...")
        loader.load_parquet_to_postgres("data/processed")
        
    # 4. Analyze & ML & Visualize
    if args.step in ['all', 'analyze']:
        logger.info("Starting Analysis & ML Step...")
        
        # Initialize Predictor
        predictor = ml.IncidentPredictor()
        
        # Train generic Classification model (High Priority prediction)
        logger.info("Fetching training data...")
        df = predictor.fetch_data(limit=50000)
        
        if not df.empty:
            logger.info("Training Classifier...")
            predictor.train_classification_model(df)
            
            logger.info("Training Regressor...")
            predictor.train_volume_regression(df)
            
            logger.info("Generating Visualizations...")
            generator.plot_heatmap(df)
            generator.plot_incident_trends(df, "Combined")
            
            # Mining
            logger.info("Running Association Rule Mining...")
            miner = mining.AssociationRuleMiner(min_support=0.01, min_confidence=0.5)
            # Use same DF for mining transactions if applicable, or fetch fresh
            # For demo, we just print status
            # transactions = miner.load_transactions() 
            # miner.get_frequent_itemsets(transactions)
            # rules = miner.generate_rules()
            # print(f"Discovered {len(rules)} rules.")
        else:
            logger.warning("No data found in DB to analyze. Please ensure 'load' step ran successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CrimeCastNYC Data Pipeline")
    parser.add_argument("--step", choices=['download', 'clean', 'load', 'analyze', 'all'], default='all', help="Pipeline step to run")
    parser.add_argument("--start_year", type=int, default=2020, help="Start year for download")
    parser.add_argument("--end_year", type=int, default=2024, help="End year for download")
    parser.add_argument("--limit", type=int, help="Limit rows for download (testing)")
    
    args = parser.parse_args()
    run_pipeline(args)
