import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import Ridge, Lasso, LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, accuracy_score, classification_report
import joblib
import os
from src.etl.loader import get_connection

class IncidentPredictor:
    def __init__(self):
        self.model = None
        self.pipeline = None
        
    def fetch_data(self, limit=100000):
        """Fetches data from Postgres/SQLite for ML training."""
        conn = get_connection()
        
        # Adjust query for SQLite (no EXTRACT)
        import sqlite3
        is_sqlite = isinstance(conn, sqlite3.Connection)
        
        if is_sqlite:
            # Use substr for hour extraction (HH:MM:SS -> HH)
            query = f"""
                SELECT 
                    incident_date, 
                    cast(substr(incident_time, 1, 2) as int) as hour,
                    cast(strftime('%w', incident_date) as int) as day_of_week,
                    precinct_id, 
                    borough, 
                    latitude, 
                    longitude,
                    complaint_type
                FROM calls_for_service
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                LIMIT {limit}
            """
        else:
            query = f"""
                SELECT 
                    incident_date, 
                    EXTRACT(HOUR FROM incident_time) as hour,
                    EXTRACT(DOW FROM incident_date) as day_of_week,
                    precinct_id, 
                    borough, 
                    latitude, 
                    longitude,
                    complaint_type
                FROM calls_for_service
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                LIMIT {limit}
            """

        try:
            df = pd.read_sql(query, conn)
            # Create target for classification (Priority)
            high_priority = ['MURDER', 'RAPE', 'ROBBERY', 'FELONY ASSAULT', 'BURGLARY', 'GRAND LARCENY', 'GRAND LARCENY OF MOTOR VEHICLE']
            df['is_high_priority'] = df['complaint_type'].apply(lambda x: 1 if any(c in str(x).upper() for c in high_priority) else 0)
            return df
        finally:
            conn.close()

    def build_regression_pipeline(self, regularization='ridge', alpha=1.0):
        """Builds a regression pipeline to predict hourly incident volume."""
        # Feature Engineering: 
        # We need to aggregate first for regression (count per hour/precinct)
        pass 

    def train_classification_model(self, df):
        """Trains a classifier to predict High Priority crimes."""
        
        X = df[['hour', 'day_of_week', 'precinct_id', 'borough', 'latitude', 'longitude']]
        y = df['is_high_priority']
        
        if y.nunique() < 2:
            print(f"Skipping Classification: Only {y.nunique()} class present in target (Needs 2).")
            return
        
        # Preprocessing
        numeric_features = ['hour', 'day_of_week', 'latitude', 'longitude']
        categorical_features = ['borough', 'precinct_id']
        
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numeric_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ])
            
        # Pipeline with Regularized Logistic Regression
        # Updated to compatible kwargs for sklearn 1.x
        self.pipeline = Pipeline([
            ('preprocessor', preprocessor),
            ('classifier', LogisticRegression(C=1.0, max_iter=1000)) 
        ])
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        print("Training Classification Model...")
        self.pipeline.fit(X_train, y_train)
        
        y_pred = self.pipeline.predict(X_test)
        print("Model Accuracy:", accuracy_score(y_test, y_pred))
        print(classification_report(y_test, y_pred))
        
        # Save model
        os.makedirs("models", exist_ok=True)
        joblib.dump(self.pipeline, "models/crime_classifier.joblib")
        
    def train_volume_regression(self, df):
        """Predicts incident volume per precinct/hour."""
        # Debug
        print("Data Frame Info:")
        print(df.info())
        print(df.head())
        
        # Aggregation
        hourly_counts = df.groupby(['incident_date', 'hour', 'precinct_id', 'borough']).size().reset_index(name='incident_count')
        
        print(f"Hourly Counts Shape: {hourly_counts.shape}")
        if hourly_counts.empty:
            print("Hourly counts empty! Check grouping keys.")
            return

        X = hourly_counts[['hour', 'precinct_id', 'borough']]
        y = hourly_counts['incident_count']
        
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore'), ['borough', 'precinct_id'])
            ], remainder='passthrough')
            
        # Ridge Regression (L2 Regularization)
        self.pipeline = Pipeline([
            ('preprocessor', preprocessor),
            ('regressor', Ridge(alpha=1.0))
        ])
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        print("Training Regression Model (Ridge)...")
        self.pipeline.fit(X_train, y_train)
        
        y_pred = self.pipeline.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        print(f"RMSE: {rmse}")
        
        joblib.dump(self.pipeline, "models/volume_regressor.joblib")

if __name__ == "__main__":
    predictor = IncidentPredictor()
    # Mock run - in real scenario would fetch real data
    # data = predictor.fetch_data()
    # predictor.train_classification_model(data)
    print("ML Module Initialized.")
