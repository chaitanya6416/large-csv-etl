import os
import pandas as pd
from datetime import datetime

# Assuming your 'etl' and 'config' modules are in the python path
from etl.chunk_processor import process_chunk

def test_process_chunk(tmp_path, monkeypatch):
    monkeypatch.setattr('config.TEMP_FILES_DIR', str(tmp_path))
    data = {
        'transaction_id': ['t1', 't2', 't3', 't4', 't5', 't6'],
        'user_id': ['u1', 'u2', 'u3', 'u4', 'u5', 'u6'],
        'amount': [100, -50, 200, 150, 'invalid', 300],
        'timestamp': [datetime.now().isoformat()] * 6,
        'status': ['SUCCESS', 'COMPLETED', 'cancelled', 'Success', 'COMPLETED', 'pending']
    }
    sample_chunk = pd.DataFrame(data)

    result_path = process_chunk(chunk_number=1, chunk=sample_chunk)
    assert os.path.exists(result_path)

    processed_df = pd.read_csv(result_path)

    assert len(processed_df) == 3, "Should keep 3 rows after filtering and error handling"
    assert 'cancelled' not in processed_df['status'].values
    assert not (processed_df['amount'] < 0).any()
    assert 't5' not in processed_df['transaction_id'].values
    assert all(s.islower() for s in processed_df['status'])
    assert 'processed_at' in processed_df.columns
    expected_ids = {'t1', 't4', 't6'}
    assert set(processed_df['transaction_id']) == expected_ids
