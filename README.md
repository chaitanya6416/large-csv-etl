Scalable CSV Processing Pipeline
A high-performance ETL pipeline designed to process large CSV files by reading data in chunks, transforming it in parallel, and loading the cleaned data into a SQLite database.

Key Features
Out-of-Core Processing: Reads and processes large CSV files in manageable chunks without loading the entire file into memory.

Parallel Execution: Utilizes a ProcessPoolExecutor for parallel data transformation on multi-core systems.

Configuration-Driven: Transformation logic is defined in a central config.py file.

Robust and Scalable: Employs memory-safe file merging and efficient database batch loading.

Command-Line Interface: Provides a CLI for overriding default configurations like file paths, chunk size, and worker count.

Automated Testing: Includes a test suite using pytest.

Project Structure
large_csv_processor/
├── data/ # For input, output, and temporary files
├── etl/ # Core ETL modules (processing, db loading, etc.)
├── logs/ # Application log files
├── tests/ # Automated test suite
├── config.py # Central configuration for the pipeline
└── main.py # Main entry point for the application

Setup and Installation
Clone the repository and navigate to the project directory.

Create and activate a virtual environment (recommended):

python -m venv venv
source venv/bin/activate # On Windows: venv\Scripts\activate

Install dependencies:

pip install -r requirements.txt

(Note: You will need to create a requirements.txt file containing pandas and pytest)

Usage
Running the ETL Pipeline
Execute the main script from the project root directory. This will use the default settings from config.py and generate a dummy data file on the first run.

python main.py

Running with Custom Arguments
You can override the default settings using command-line arguments.

# Example: Run with 4 workers and a chunk size of 50,000

python main.py --workers 4 --chunk-size 50000

Use python main.py --help to see all available options.

Running the Tests
To verify the integrity of the application, run the test suite using pytest.

# Run all tests

pytest

# Run with verbose output

pytest -v

Configuration
All key parameters (file paths, chunk size, worker count, transformation rules) can be easily modified in the config.py file.
