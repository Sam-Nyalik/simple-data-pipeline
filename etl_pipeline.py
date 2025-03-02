import pandas as pd
import sqlite3

# Step 1: Extract
def extract(file_path):
    # Extract data from a csv file
    print("Extracting data...")
    data = pd.read_csv(file_path)
    return data

# Step 2: Transform
def transform(data):
    # Cleaning and enriching the data
    print("Transforming data...")
    # Drop the rows with missing prices
    data = data.dropna(subset=['price'])
    # Convert price to a float
    data['price'] = data['price'].astype(float)
    # Calculate the total sales for each product
    data['total_sales'] = data['price'] * data['quantity']
    return data

# Step 3: Load
def load(data, db_path, table_name):
    # Load data into an SQLite database
    print("Loading data into the database...")
    conn = sqlite3.connect(db_path)
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print("Data loaded successfully!")
    
# Main ETL Pipeline
def main():
    # Define the file paths
    csv_file = "data/sales_data.csv"
    database_file ="database/sales.db"
    table_name = "sales"
    
    # Run the ETL steps
    data = extract(csv_file)
    clean_data = transform(data)
    load(clean_data, database_file, table_name)
    
if __name__ == "__main__":
    main()
    