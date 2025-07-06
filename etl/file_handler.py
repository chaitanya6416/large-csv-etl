import os 

import config

def merge_temp_files(temp_file_paths):
    final_csv_path = config.FINAL_CSV_PATH
    # os.makedirs(final_csv_path, exist_ok=True)
    try:
        with open(final_csv_path, 'w', newline='') as outfile:
            with open(temp_file_paths[0], 'r') as f:
                header = f.readline()
                outfile.write(header)
                for line in f:
                    outfile.write(line)

            for i in range(1, len(temp_file_paths)):
                with open(temp_file_paths[i], 'r') as f:
                    next(f)  # Skip header line
                    for line in f:
                        outfile.write(line)
        
        print(f"Successfully merged data to {final_csv_path}.")

    except Exception as e:
        print(f"Failed to merge temporary files: {e}", exc_info=True)
        return ""

    finally:
        # delete temp files and the temp folder
        for file in temp_file_paths:
            try:
                os.remove(file)
            except OSError as e:
                print(f"Error removing temporary file {file}: {e}")
        try:
            os.rmdir(config.TEMP_FILES_DIR)
            print("Cleaned up temporary files.")
        except OSError as e:
            print(f"Error removing temporary directory. msg: {e}")
    
    return final_csv_path
