#!/usr/bin/env python3

import pandas as pd # Datas manipulation and export csv
from sqlalchemy import create_engine, text # SQLAlchemy engine and text queries
import pymysql # MySQL driver for SQLAlchemy
from pathlib import Path # Path indicator
from dotenv import load_dotenv # Env manipulation
import os # OS interaction

# Load datas from .env
load_dotenv()

# Crée une URL de connexion SQLAlchemy
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")


# Find this file path 
base_dir = Path(__file__).parent
# Define the path of "CSV" dir
csv_dir = base_dir / "CSV"
# Create a CSV dir if needed
csv_dir.mkdir(exist_ok=True)


with engine.connect() as conn: # Open a connection safely
	result = conn.execute(
		text("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE';")
	)
	# Extract table names from the result set
	tables = [row[0] for row in result.fetchall()]

print(f"{len(tables)} table(s) found in the database '{database}'\n")


for table in tables:
	print(f"Exporting table: {table}")
	# Read the table into a Pandas DataFrame using the SQLAlchemy engine
	df = pd.read_sql(f"SELECT * FROM `{table}`", engine)
	# Define the path for this CSV file
	out_path = csv_dir / f"{table}.csv"
	# Write the DataFrame to a CSV file
	df.to_csv(out_path, index=False)
	print(f"File created: {out_path.name} ({len(df)} rows)\n")

engine.dispose()  # Close all connections

print(f"All CSV files have been saved in: {csv_dir.resolve()}")
