# first party imports
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
from datetime import datetime
import logging
from math import ceil
import os
import pandas as pd
import argparse

# third party imports
import config
from etl.chunk_processor import process_chunk
from etl.db_loader import load_csv_to_db
from etl.file_handler import merge_temp_files
from etl.logger_config import setup_logging

logger = logging.getLogger(__name__)

def create_dummy_data():
    """Creates dummy data based on settings in the config module."""
    if os.path.exists(config.TRANSACTIONS_CSV_PATH):
        logger.info("skipping creating of dummy data")
    else:
        os.makedirs(config.INPUT_DATA_DIR, exist_ok=True)
        with open(config.TRANSACTIONS_CSV_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(config.get_csv_headers())
            bytes_count = config.SAMPLE_DATA_FILE_SIZE * 1024 * 1024 * 1024
            rows = ceil(bytes_count // 70)
            for i in range(1, rows+1):
                match i%4:
                    case 0:
                        writer.writerow([f"t_{i}", f"u_{i}", 100, datetime.now().isoformat(), "success"])
                    case 1:
                        writer.writerow([f"t_{i}", f"u_{i}", 100, datetime.now().isoformat(), "fail"])
                    case 2:
                        writer.writerow([f"t_{i}", f"u_{i}", 100, datetime.now().isoformat(), "Success"])
                    case _:
                        writer.writerow([f"t_{i}", f"u_{i}", 100, datetime.now().isoformat(), "Fail"])
                match i%101:
                    case 0:
                        writer.writerow([f"u_{i}", 100, datetime.now().isoformat(), "fail"])
                    case 1:
                        writer.writerow([""])
        logger.info(f"created a dummy file with size ~{config.SAMPLE_DATA_FILE_SIZE}GB")


def run_etl_pipeline():
    """Runs the core ETL logic using settings from the config module."""
    temp_file_paths = []
    os.makedirs(config.TEMP_FILES_DIR, exist_ok=True)
    logger.info(f"spinning up {config.WORKERS} number of workers.")
    with ProcessPoolExecutor(max_workers=config.WORKERS) as exec:
        csv_iterator = pd.read_csv(config.TRANSACTIONS_CSV_PATH, chunksize=config.CHUNK_SIZE, on_bad_lines='warn', names=config.get_csv_headers(), header=0)
        futures = [
            exec.submit(process_chunk, chunk_number, chunk)
            for chunk_number, chunk in enumerate(csv_iterator)
        ]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                temp_file_paths.append(result)
    return temp_file_paths


def main():
    parser = argparse.ArgumentParser(description="Large CSV ETL and Database Loader.")
    parser.add_argument("--input-file", type=str, help=f"Path to the input CSV file. Default: {config.TRANSACTIONS_CSV_PATH}")
    parser.add_argument("--db-path", type=str, help=f"Path to the SQLite database file. Default: {config.DB_PATH}")
    parser.add_argument("--chunk-size", type=int, help=f"Number of rows per chunk. Default: {config.CHUNK_SIZE}")
    parser.add_argument("--workers", type=int, help=f"Number of parallel processes to use. Default: {config.WORKERS}")
    parser.add_argument("--file-size", type=float, help=f"Size of the dummy data file to create in GB. Default: {config.SAMPLE_DATA_FILE_SIZE}")
    
    args = parser.parse_args()

    if args.input_file:
        config.TRANSACTIONS_CSV_PATH = args.input_file
        config.INPUT_DATA_DIR = os.path.dirname(args.input_file)
    if args.db_path:
        config.DB_PATH = args.db_path
    if args.chunk_size:
        config.CHUNK_SIZE = args.chunk_size
    if args.workers:
        config.WORKERS = args.workers
    if args.file_size:
        config.SAMPLE_DATA_FILE_SIZE = args.file_size
    
    setup_logging()
    logger.info("Starting ETL Pipeline with final configuration.")
    logger.info(f"Input file: {config.TRANSACTIONS_CSV_PATH}")
    logger.info(f"Database path: {config.DB_PATH}")
    logger.info(f"Chunk size: {config.CHUNK_SIZE}, Workers: {config.WORKERS}")

    create_dummy_data()
    temp_file_paths = run_etl_pipeline()
    
    if temp_file_paths:
        final_csv_path = merge_temp_files(temp_file_paths)
        if final_csv_path:
            load_csv_to_db(final_csv_path)
    else:
        logger.warning("No temporary files were created. ETL process finished without generating output.")


if __name__ == "__main__":
    main()
