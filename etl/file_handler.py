# first party imports
import os 
import logging 

# third party imports
import config

logger = logging.getLogger(__name__)


def merge_temp_files(temp_file_paths):
    final_csv_path = config.FINAL_CSV_PATH
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
        
        logger.info(f"Successfully merged data to {final_csv_path}.")

    except Exception as e:
        logger.error(f"Failed to merge temporary files: {e}", exc_info=True)
        return ""

    finally:
        # delete temp files and the temp folder
        for file in temp_file_paths:
            try:
                os.remove(file)
            except OSError as e:
                logger.error(f"Error removing temporary file {file}: {e}")
        try:
            os.rmdir(config.TEMP_FILES_DIR)
            logger.info("Cleaned up temporary files.")
        except OSError as e:
            logger.error(f"Error removing temporary directory. msg: {e}")
    
    return final_csv_path
