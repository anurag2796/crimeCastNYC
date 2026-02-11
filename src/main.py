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
        logger.info("Fetching training data (Full Dataset)...")
        # Use args.limit if provided, else None for full DB
        limit = args.limit if args.limit else None 
        df = predictor.fetch_data(limit=limit)
        
        if not df.empty:
            logger.info("Training Classifier...")
            predictor.train_classification_model(df)
            
            logger.info("Training Regressor...")
            predictor.train_volume_regression(df)
            
            logger.info("Generating Visualizations...")
            generator.plot_heatmap(df)
            generator.plot_incident_trends(df, "2024 Data")
            generator.plot_crime_by_borough(df)
            generator.plot_top_crime_types(df)
            generator.plot_hourly_distribution(df)
            generator.plot_spatial_scatter(df)
            generator.plot_priority_distribution(df)
            
            # Mining
            logger.info("Running Association Rule Mining (Optimized)...")
            miner = mining.AssociationRuleMiner(min_support=0.001, min_confidence=0.01)
            basket = miner.load_transactions_df()
            if basket is not None:
                rules_df = miner.mine_rules(basket)
                logger.info(f"Discovered {len(rules_df)} rules.")
                if not rules_df.empty:
                    print(rules_df.head())
                    generator.plot_association_rules(rules_df)
            else:
                logger.warning("No transactions found for mining.")
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
