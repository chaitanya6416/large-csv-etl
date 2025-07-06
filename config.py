import os 

WORKERS = 6
INPUT_DATA_CSV_PATH = os.path.join(os.getcwd(), "data", "input")
TRANSACTIONS_CSV_PATH = os.path.join(INPUT_DATA_CSV_PATH, "transactions.csv")
SAMPLE_DATA_FILE_SIZE = 5 #in GB
CHUNK_SIZE = 1_000

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
