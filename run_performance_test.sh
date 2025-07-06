echo "Starting ETL Performance Test Suite..."
echo "-------------------------------------"

WORKER_COUNTS=(1 2 4 8)
CHUNK_SIZES=(10000 50000 100000 200000)
DUMMY_FILE_SIZE_GB=1

LOG_FILE="performance_results.log"
echo "Performance Test Results" > "$LOG_FILE"
echo "Test run on: $(date)" >> "$LOG_FILE"
echo "=================================================================" >> "$LOG_FILE"

echo "Performing initial setup..."
echo "Cleaning all data directories..."
rm -rf data/input/* data/output/* logs/*

echo "Creating the ${DUMMY_FILE_SIZE_GB}GB dummy data file. This may take a moment..."

python main.py --workers 4 --chunk-size 10000 --file-size $DUMMY_FILE_SIZE_GB >/dev/null 2>/dev/null
echo "Dummy data file created successfully."
echo ""


TABLE_HEADER=$(printf "%-10s | %-12s | %-12s | %-12s | %-12s" "Workers" "Chunk Size" "Real Time" "User Time" "Sys Time")
echo "$TABLE_HEADER"
echo "-----------------------------------------------------------------"

echo "$TABLE_HEADER" >> "$LOG_FILE"
echo "-----------------------------------------------------------------" >> "$LOG_FILE"


for workers in "${WORKER_COUNTS[@]}"; do
  for chunk_size in "${CHUNK_SIZES[@]}"; do
    
    rm -rf data/output/* logs/*

    TIME_OUTPUT=$({ time python main.py --workers $workers --chunk-size $chunk_size --file-size $DUMMY_FILE_SIZE_GB >/dev/null 2>/dev/null; } 2>&1)

    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    USER_TIME=$(echo "$TIME_OUTPUT" | grep user | awk '{print $2}')
    SYS_TIME=$(echo "$TIME_OUTPUT" | grep sys | awk '{print $2}')

    RESULT_ROW=$(printf "%-10s | %-12s | %-12s | %-12s | %-12s" "$workers" "$chunk_size" "$REAL_TIME" "$USER_TIME" "$SYS_TIME")
    
    echo "$RESULT_ROW"
    echo "$RESULT_ROW" >> "$LOG_FILE"

  done
done

echo ""
echo "====================================="
echo "All performance tests completed."
echo "Tabular results have been saved to $LOG_FILE"
echo "====================================="
