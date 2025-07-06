# first party imports
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
from datetime import datetime
import os
import pandas as pd


# third party imports
import config


def create_dummy_data():
    if os.path.exists(config.TRANSACTIONS_CSV_PATH):
        print("skipping creating of dummy data")
    else:
        os.makedirs(config.INPUT_DATA_CSV_PATH, exist_ok=True)
        with open(config.TRANSACTIONS_CSV_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(config.get_csv_headers())
            bytes_count = config.SAMPLE_DATA_FILE_SIZE * 1024 * 1024 * 1024
            rows = bytes_count // 100
            for i in range(1, rows+1):
                writer.writerow([f"t_{i}", f"u_{i}", 100, datetime.now().isoformat(), "success"])


def run_etl_pipeline():
    temp_file_paths = []
    with ProcessPoolExecutor(max_workers=config.WORKERS) as exec:
        futures = [
            exec.submit(process_chunk, chunk)
            for chunk in pd.read_csv(config.INPUT_DATA_CSV_PATH, chunksize=config.CHUNK_SIZE)
        ]
        
        for future in as_completed(futures):
            temp_file_paths.append(future.result())
    return temp_file_paths


def main():
    create_dummy_data()
    temp_file_paths = run_etl_pipeline()
    merge_temp_files(temp_file_paths)


if __name__ == "__main__":
    main()
