# Large size csv Processing Pipeline

A pipeline designed to process large CSV files by reading data in chunks, transforming it in parallel, and loading the cleaned data into a SQLite database.

## Design

![alt text](images/image.png)

## Key Features

- Processes in chunks: Reads and processes large CSV files in manageable chunks without loading the entire file into memory.
- Parallel Execution: Utilizes a ProcessPoolExecutor for parallel data transformation on multi-core systems.
- Configuration-Driven: Transformation logic is defined in a central config.py file.
- Command-Line Interface: Provides a CLI for overriding default configurations like file paths, chunk size, and worker count.

## Project Structure

large_csv_etl/ <br/>

- data/ # For input, output, and temporary files <br/>
- etl/ # Core ETL modules (processing, db loading, etc.) <br/>
- logs/ # Application log files <br/>
- tests/ # Automated test suite <br/>
- config.py # Central configuration for the pipeline <br/>
- main.py # Main entry point for the application <br/>

## Setup and Installation

Clone the repository and navigate to the project directory.

Create and activate a virtual environment (recommended):

`python -m venv venv` <br/>
`source venv/bin/activate`

Install dependencies:

`pip install -r requirements.txt`

## Usage

Running the ETL Pipeline
Execute the main script from the project root directory. This will use the default settings from config.py and generate a dummy data file on the first run.

`python main.py`

## Running with Custom Arguments

You can override the default settings using command-line arguments.

Example: Run with 4 workers and a chunk size of 50,000

`python main.py --workers 4 --chunk-size 50000`

Use `python main.py --help` to see all available options.

![alt text](images/image-1.png)

## Running the Tests <br/>

To verify the integrity of the application, run the test suite using pytest.

pytest

## Performance testing

The script `run_performance_test.sh` is created to check the running times of the entire etl process for different number of workers & different chunk sizes. <br/>

The below screenshot depicts the performance results for a 1GB size of data file. <br/>
![alt text](images/image-2.png)

Check the .sh file for more details.

## Configuration

All key parameters (file paths, chunk size, worker count, transformation rules) can be easily modified in the config.py file.
