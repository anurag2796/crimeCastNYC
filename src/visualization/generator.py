import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

OUTPUT_DIR = "data/output/plots"

def ensure_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def plot_incident_trends(df, year):
    """Plots daily incident counts over time."""
    ensure_dir()
    daily_counts = df.groupby('incident_date').size()
    
    plt.figure(figsize=(12, 6))
    daily_counts.plot(kind='line', color='teal')
    plt.title(f'Daily Call Volume Trend - {year}')
    plt.xlabel('Date')
    plt.ylabel('Number of Calls')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/trend_{year}.png")
    plt.close()

def plot_heatmap(df):
    """Plots heatmap of incidents by Day of Week and Hour."""
    ensure_dir()
    if 'hour' not in df.columns:
        df['hour'] = pd.to_datetime(df['incident_time'], format='%H:%M:%S').dt.hour
        
    if 'day_name' not in df.columns:
        df['day_name'] = pd.to_datetime(df['incident_date']).dt.day_name()
    
    pivot_table = df.pivot_table(index='day_name', columns='hour', aggfunc='size', fill_value=0)
    
    # Reorder days
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot_table = pivot_table.reindex(days_order)
    
    plt.figure(figsize=(14, 8))
    sns.heatmap(pivot_table, cmap='YlOrRd', linewidths=0.5)
    plt.title('Crime Hotspots: Day vs Hour')
    plt.xlabel('Hour of Day')
    plt.ylabel('Day of Week')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/heatmap_day_hour.png")
    plt.close()

def plot_regression_results(y_true, y_pred):
    """Scatter plot of Actual vs Predicted."""
    ensure_dir()
    plt.figure(figsize=(8, 8))
    plt.scatter(y_true, y_pred, alpha=0.5)
    
    # Perfect fit line
    max_val = max(max(y_true), max(y_pred))
    plt.plot([0, max_val], [0, max_val], 'r--')
    
    plt.title('Prediction Accuracy (Actual vs Predicted)')
    plt.xlabel('Actual Volume')
    plt.ylabel('Predicted Volume')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/regression_accuracy.png")
    plt.close()

if __name__ == "__main__":
    print("Visualization Engine ready.")
