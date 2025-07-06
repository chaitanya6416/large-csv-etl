# first party imports
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
from datetime import datetime
from math import ceil
import os
import pandas as pd


# third party imports
import config
from etl.chunk_processor import process_chunk
from etl.file_handler import merge_temp_files
from etl.db_loader import load_csv_to_db

def create_dummy_data():
    if os.path.exists(config.TRANSACTIONS_CSV_PATH):
        print("skipping creating of dummy data")
    else:
        os.makedirs(config.INPUT_DATA_DIR, exist_ok=True)
        with open(config.TRANSACTIONS_CSV_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(config.get_csv_headers())
            bytes_count = config.SAMPLE_DATA_FILE_SIZE * 1024 * 1024 * 1024
            rows = ceil(bytes_count // 70) # an approximate computation based on no.of bytes per row
            print("rows == ", rows)
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
                        writer.writerow([f"u_{i}", 100, datetime.now().isoformat(), "fail"]) # missing transaction id
                    case 1:
                        writer.writerow([""]) # empty row

def run_etl_pipeline():
    temp_file_paths = []
    os.makedirs(config.TEMP_FILES_DIR, exist_ok=True)
    with ProcessPoolExecutor(max_workers=config.WORKERS) as exec:
        futures = [
            exec.submit(process_chunk, chunk)
            for chunk in pd.read_csv(config.TRANSACTIONS_CSV_PATH, chunksize=config.CHUNK_SIZE)
        ]
        
        for future in as_completed(futures):
            temp_file_paths.append(future.result())
    return temp_file_paths


def main():
    create_dummy_data()
    temp_file_paths = run_etl_pipeline()
    final_csv_path = merge_temp_files(temp_file_paths)
    load_csv_to_db(final_csv_path)
    


if __name__ == "__main__":
    main()

# rows after processing == 1,533,916
# rows befor processing == 1,564,291