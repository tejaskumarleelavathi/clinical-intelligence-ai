"""
Step 0 of ETL: Export SQLite tables to CSV files.
Run this locally. Output goes to raw-data/ (then upload to ADLS Bronze).
"""
import sqlite3
import os

import pandas as pd

# --- CONFIG: Change these to match your environment ---
DB_PATH = "/Users/tejas/Documents/GitHub/clinical-intelligence-ai/raw_data/medical_data.db"   # e.g. "../data/clinical.db" or absolute path
OUTPUT_DIR = "raw-data/"
# ----------------------------------------------------

# Create output folder if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# The 11 tables we care about (sqlite_sequence is internal — we skip it)
TABLES = [
    "patients",
    "medications",
    "lab_results",
    "allergies",
    "immunizations",
    "care_providers",
    "progress_notes",
    "recent_visits",
    "medical_history",
    "medical_history_type",
    "diagnostic_imaging",
]


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        for table in TABLES:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
            df.to_csv(filepath, index=False)
            print(f"Exported {table}: {len(df)} rows -> {filepath}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()