# CrimeCastNYC 2.0 - Advanced Crime Analytics Platform

**CrimeCastNYC 2.0** is a scalable data engineering and analytics platform designed to ingest, process, and analyze 20+ years of NYPD 911 Call Data. It leverages Apache Spark for big data processing, PostgreSQL for structured storage, and Machine Learning for predictive insights.

## Architecture

1.  **Ingestion Layer**: 
    -   Automated downloader using Socrata Open Data API.
    -   Fetches data chunked by year (2004-2024).

2.  **Processing Layer (ETL)**:
    -   **Apache Spark (PySpark)**: Cleans and normalizes raw CSV data into optimized Parquet metrics.
    -   **PostgreSQL Loader**: Bulk loads processed data into a **Partitioned Star Schema** (partitioned by year) for high-performance querying.

3.  **Analytics Core**:
    -   **Association Rule Mining**: Custom Apriori algorithm to discover crime co-occurrence patterns (e.g., "If simple assault happens in Precinct 75 at 2 AM, robbery is likely").
    -   **Machine Learning**:
        -   **Classification**: Predicts High/Low priority crimes based on spatiotemporal features (Logistic Regression with L2 Regularization).
        -   **Regression**: Predicts call measure volume per precinct/hour (Ridge Regression).

4.  **Visualization**:
    -   Automated generation of Static Reports (Time Series, Heatmaps, Prediction Scatters).

## Prerequisities

-   **Docker & Docker Compose**
-   **Python 3.10+**
-   **Poetry** (or pip)

## Setup

1.  **Initialize Environment**:
    ```bash
    poetry install
    ```

2.  **Start Database**:
    ```bash
    docker-compose up -d
    ```
    *This starts a PostgreSQL 15 instance optimized for large datasets.*

## Usage

 The entire pipeline is orchestrated via `src/main.py`.

### 1. Full Pipeline Run (Download -> Clean -> Load -> Analyze)
```bash
poetry run python src/main.py --step all --start_year 2023 --end_year 2024
```

### 2. Individual Steps

**Download Data (Raw CSVs)**
```bash
poetry run python src/main.py --step download --start_year 2023 --end_year 2023
```

**Clean Data (Spark -> Parquet)**
```bash
poetry run python src/main.py --step clean --input data/raw --output data/processed
```

**Load Data (Parquet -> Postgres)**
```bash
poetry run python src/main.py --step load
```

**Run Analytics & ML**
```bash
poetry run python src/main.py --step analyze
```

## Directory Structure

```
crimeCastNYC/
├── legacy/                  # Archived Phase 1-3 code
├── data/
│   ├── raw/                 # Landing zone for CSVs
│   ├── processed/           # Spark output (Parquet)
│   └── output/              # Generated Plots & Models
├── src/
│   ├── etl/                 # Downloader, Cleaner, Loader
│   ├── db/                  # DB Schemas
│   ├── analysis/            # ML & Mining Logic
│   └── visualization/       # Plot Generators
├── docker/                  # Docker Configs
└── pyproject.toml           # Python Dependencies
```

## Credits

**Project Lead & Restructuring:**
- Anurag Lnu (al5150@g.rit.edu)

**Original Phase Contributors:**
- Atharva Rajan Kale
- Kirubhakaran Meenakshi Sundaram
- Kushagra Gupta

*This project builds upon initial work completed in collaboration with the above contributors. The current version (CrimeCastNYC 2.0) represents a complete architectural restructuring and enhancement.*
