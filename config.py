import os 

base_dir = os.getcwd()

# directories
INPUT_DATA_DIR = os.path.join(os.getcwd(), "data", "input")
OUTPUT_DIR = os.path.join(base_dir, "data", "output")
TEMP_FILES_DIR = os.path.join(OUTPUT_DIR, "temp")

# files
TRANSACTIONS_CSV_PATH = os.path.join(INPUT_DATA_DIR, "transactions.csv")
FINAL_CSV_PATH = os.path.join(OUTPUT_DIR, "processed_transactions.csv")
DB_PATH = os.path.join(OUTPUT_DIR, "transactions.db")
DB_TABLE_NAME = "transactions"

# algorithm configs
WORKERS = 6
SAMPLE_DATA_FILE_SIZE = 0.1 #in GB
CHUNK_SIZE = 10_000

# others
def get_csv_headers():
    return ["transaction_id", "user_id", "amount", "timestamp", "status"]

def get_transformation_config():
    return [
        {"action": "coerce_to_numeric", "field_name": "amount"},
        {"action": "standardize_column", "field_name": "status", "type": "lower"},
        {"action": "filter_rows", "field_name": "status", "condition": "not_equal", "value": "cancelled"},
        {"action": "filter_rows", "field_name": "amount", "condition": "greater_than_or_equal_to", "value": 0},
        {"action": "add_column", "field_name": "processed_at", "value_type": "utcnow"}
    ]
