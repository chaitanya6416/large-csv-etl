import logging 
import os
import pandas as pd
import sqlite3


import config

logger = logging.getLogger(__name__)

def create_connection(db_file: str) -> sqlite3.Connection:
    """Create a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger.info(f"Successfully connected to SQLite database: {db_file}")
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise
    return conn


def db_data_load_helper(conn: sqlite3.Connection, file_path: str, table_name: str):
    """
    Loads data from a CSV file into a SQLite table in batches.
    """
    logger.info(f"Loading data from {file_path} into table '{table_name}'...")
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
            logger.info(f"Loaded batch {i+1} ({len(chunk)} rows) into '{table_name}'.")
        
        logger.info("Database loading complete.")

    except Exception as e:
        logger.error(f"Failed to load data into database: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def load_csv_to_db(final_csv_path):
    if final_csv_path:
        try:
            conn = create_connection(config.DB_PATH)
            # Drop table for a clean run each time
            conn.execute(f"DROP TABLE IF EXISTS {config.DB_TABLE_NAME}")
            conn.commit()
            db_data_load_helper(conn, final_csv_path, config.DB_TABLE_NAME)
        except Exception as e:
            logger.error(f"Database loading failed: {e}")
    else:
        logger.warning("No final_csv_path given to dump to db")
