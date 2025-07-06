import os
import pandas as pd
import sqlite3


import config

def create_connection(db_file: str) -> sqlite3.Connection:
    """Create a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Successfully connected to SQLite database: {db_file}")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}", exc_info=True)
        raise
    return conn


def db_data_load_helper(conn: sqlite3.Connection, file_path: str, table_name: str):
    """
    Loads data from a CSV file into a SQLite table in batches.
    """
    if not os.path.exists(file_path):
        print(f"Data file not found at {file_path}. Cannot load to database.")
        return

    print(f"Loading data from {file_path} into table '{table_name}'...")
    try:
        chunk_size = 10000 
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            chunk.to_sql(
                table_name,
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            print(f"Loaded batch {i+1} ({len(chunk)} rows) into '{table_name}'.")
        
        print("Database loading complete.")

    except Exception as e:
        print(f"Failed to load data into database: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


def load_csv_to_db(final_csv_path):
    if final_csv_path:
        try:
            conn = create_connection(config.DB_PATH)
            # Drop table for a clean run each time
            conn.execute(f"DROP TABLE IF EXISTS {config.DB_TABLE_NAME}")
            conn.commit()
            db_data_load_helper(conn, final_csv_path, config.DB_TABLE_NAME)
        except Exception as e:
            print(f"Database loading failed: {e}")
    else:
        print("Merging failed, skipping database load.")
    pass
