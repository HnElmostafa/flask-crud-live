from google.cloud import bigquery
from sqlalchemy import create_engine
import pandas as pd

# Configure your PostgreSQL connection
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "flask_db"
DB_PORT = "5432"
DB_NAME = "postgres"


# Create PostgreSQL connection string
POSTGRES_URI = "postgresql://postgres:postgres@flask_db:5432/postgres"

engine = create_engine(POSTGRES_URI)


# Initialize BigQuery client
bq_client = bigquery.Client()

def upload_bigquery_to_postgres():
    dataset_id = "spry-sequence-454215-t5.my_dataset"  # BigQuery Dataset
    table_id = "pivot_data"  # BigQuery Table
    postgres_table = "resulting" 
  
    
    # BigQuery SQL Query
    query = "SELECT * FROM `spry-sequence-454215-t5.my_dataset.pivot_data`"

    df = bq_client.query(query).to_dataframe()  # Convert to Pandas DataFrame
    
    if df.empty:
        print("⚠️ No data found in BigQuery!")
        return
    
    # Insert Data into PostgreSQL

    df.to_sql("resulting", engine, if_exists='replace', index=False)

    
    print(f"✅ Data uploaded successfully to resulting in PostgreSQL!")

if __name__ == "__main__":
   

    upload_bigquery_to_postgres()
