import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

OUTPUT_DIR = "data/output/plots"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def plot_association_rules(rules_df):
    """Generates a heatmap of Association Rules by Lift."""
    if rules_df.empty:
        print("No rules to plot.")
        return

    plt.figure(figsize=(14, 10))
    # Pivot for Heatmap: Antecedent vs Consequent, value=Lift
    # Filter top 20 rules by lift to avoid overcrowding
    top_rules = rules_df.head(20)
    pivot = top_rules.pivot(index='antecedent', columns='consequent', values='lift')
    
    sns.heatmap(pivot, annot=True, cmap='coolwarm', fmt=".2f")
    plt.title("Top Association Rules (Lift Metric)")
    plt.xlabel("Consequent (Then)")
    plt.ylabel("Antecedent (If)")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/association_rules_lift.png")
    plt.close()

def plot_heatmap(df):
    """Generates a heatmap of crime frequency by Hour and Day of Week."""
    plt.figure(figsize=(12, 6))
    pivot = df.pivot_table(index='day_of_week', columns='hour', aggfunc='size', fill_value=0)
    sns.heatmap(pivot, cmap='Reds', linewidths=0.5)
    plt.title("Crime Frequency Heatmap (Day vs Hour)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Day of Week (0=Monday, 6=Sunday)")
    plt.savefig(f"{OUTPUT_DIR}/heatmap_day_hour.png")
    plt.close()

def plot_incident_trends(df, label):
    """Generates a time-series plot of incident volume."""
    # Ensure incident_date is datetime
    df['incident_date'] = pd.to_datetime(df['incident_date'])
    daily_counts = df.groupby('incident_date').size()
    
    plt.figure(figsize=(14, 7))
    daily_counts.plot(kind='line', color='blue')
    plt.title(f"Daily Incident Trends - {label}")
    plt.xlabel("Date")
    plt.ylabel("Number of Incidents")
    plt.grid(True)
    plt.savefig(f"{OUTPUT_DIR}/incident_trends.png")
    plt.close()

def plot_crime_by_borough(df):
    """Generates a bar chart of crimes by borough."""
    plt.figure(figsize=(10, 6))
    df['borough'].value_counts().plot(kind='bar', color='teal')
    plt.title("Crime Distribution by Borough")
    plt.xlabel("Borough")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/crime_by_borough.png")
    plt.close()

def plot_top_crime_types(df, n=15):
    """Generates a horizontal bar chart of top N crime types."""
    plt.figure(figsize=(12, 8))
    top_crimes = df['complaint_type'].value_counts().head(n).sort_values()
    top_crimes.plot(kind='barh', color='purple')
    plt.title(f"Top {n} Crime Types")
    plt.xlabel("Count")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/top_{n}_crime_types.png")
    plt.close()

def plot_hourly_distribution(df):
    """Generates a line plot of average crime by hour."""
    plt.figure(figsize=(10, 6))
    hourly = df.groupby('hour').size()
    hourly.plot(kind='line', marker='o', color='darkorange')
    plt.title("Total Hourly Crime Distribution")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Total Incidents")
    plt.grid(True)
    plt.xticks(range(0, 24))
    plt.savefig(f"{OUTPUT_DIR}/hourly_distribution.png")
    plt.close()

def plot_spatial_scatter(df):
    """Generates a scatter plot of crime locations (latitude/longitude)."""
    plt.figure(figsize=(10, 10))
    # Filter out potential bad lat/long data (0,0) or outside NYC approx bounds
    valid_df = df[(df['latitude'] > 40) & (df['latitude'] < 41) & (df['longitude'] > -74.3) & (df['longitude'] < -73.6)]
    sns.scatterplot(x='longitude', y='latitude', data=valid_df, hue='borough', alpha=0.3, s=10)
    plt.title("Spatial Distribution of Incidents")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend(markerscale=2)
    plt.savefig(f"{OUTPUT_DIR}/spatial_scatter.png")
    plt.close()

def plot_priority_distribution(df):
    """Generates a pie chart of High vs Low priority crimes."""
    plt.figure(figsize=(8, 8))
    counts = df['is_high_priority'].value_counts()
    labels = ['Low Priority', 'High Priority'] if len(counts) == 2 else counts.index
    plt.pie(counts, labels=labels, autopct='%1.1f%%', colors=['skyblue', 'salmon'], startangle=140)
    plt.title("High vs Low Priority Crime Distribution")
    plt.savefig(f"{OUTPUT_DIR}/priority_distribution.png")
    plt.close()
