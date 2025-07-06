from datetime import datetime
import os
import sqlite3
import pandas as pd
import pytest


from etl.db_loader import load_csv_to_db

@pytest.fixture
def db_setup(tmp_path):
    """
    Pytest fixture to set up a temporary database and a sample CSV data file.
    """
    test_data_path = tmp_path / "final_processed_data.csv"
    data = {
        'transaction_id': ['t1', 't2', 't3'],
        'user_id': ['u1', 'u2', 'u3'],
        'amount': [100, 200, 50],
        'timestamp': [datetime.now().isoformat()] * 3,
        'status': ['success', 'completed', 'success'],
        'processed_at': [datetime.now().isoformat()] * 3
    }
    pd.DataFrame(data).to_csv(test_data_path, index=False)
    
    test_db_path = tmp_path / "test_db.sqlite"
    table_name = "test_transactions"
    
    yield str(test_data_path), str(test_db_path), table_name

def test_load_csv_to_db(db_setup, monkeypatch):
    """
    Tests loading data from a final CSV file into a SQLite database.
    """
    final_csv_path, db_path, table_name = db_setup
    
    monkeypatch.setattr('config.DB_PATH', db_path)
    monkeypatch.setattr('config.DB_TABLE_NAME', table_name)

    load_csv_to_db(final_csv_path)

    assert os.path.exists(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    assert cursor.fetchone() is not None, f"Table '{table_name}' was not created."
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 3, "The number of rows in the database should match the CSV file"
    
    conn.close()
