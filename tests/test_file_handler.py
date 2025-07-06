import os
import pytest

# Assuming your 'etl' and 'config' modules are in the python path
from etl.file_handler import merge_temp_files

@pytest.fixture
def setup_temp_files(tmp_path):
    """
    Pytest fixture to create a set of temporary CSV files for testing the
    merge functionality. It handles creation and cleanup automatically.
    """
    # Create 3 temporary CSV files with headers and data
    file_paths = []
    total_data_rows = 0
    header = "col1,col2\n"
    
    # File 1
    path1 = tmp_path / "temp1.csv"
    path1.write_text(header + "a,1\nb,2\n")
    file_paths.append(str(path1))
    total_data_rows += 2
    
    # File 2
    path2 = tmp_path / "temp2.csv"
    path2.write_text(header + "c,3\n")
    file_paths.append(str(path2))
    total_data_rows += 1
    
    # File 3
    path3 = tmp_path / "temp3.csv"
    path3.write_text(header + "d,4\ne,5\nf,6\n")
    file_paths.append(str(path3))
    total_data_rows += 3
    
    yield file_paths, total_data_rows


def test_merge_temp_files(setup_temp_files, tmp_path, monkeypatch):
    temp_file_paths, total_data_rows = setup_temp_files
    final_file_path = tmp_path / "final.csv"
    
    monkeypatch.setattr('config.FINAL_CSV_PATH', str(final_file_path))
    monkeypatch.setattr('config.TEMP_FILES_DIR', str(tmp_path))

    result_path = merge_temp_files(temp_file_paths)

    assert result_path == str(final_file_path)
    assert os.path.exists(result_path)

    with open(result_path, 'r') as f:
        lines = f.readlines()
        assert len(lines) == total_data_rows + 1
        assert lines[0].strip() == "col1,col2"
        assert lines[-1].strip() == "f,6"

    for temp_file in temp_file_paths:
        assert not os.path.exists(temp_file)
