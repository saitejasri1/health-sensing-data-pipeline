# Data Pipeline
This pipeline processes raw JSON user event logs into cleaned, analytics-ready Parquet files. It is also tasked with producing summary aggregations of the data.

## Pipeline Steps
The pipeline is driven by a main function that executes the following modules:

1. Extract (extract_data)
- Reads the raw JSON file of events into memory.
- Checks that user_id, event_type, and timestamp are present and non-empty.
- Logs and discards malformed events to allow the pipeline to continue running.

2. Transform (transform_data)
- Converts valid records into a Pandas DataFrame.
- Flattens the nested metadata field into top-level columns, eliminating the need for JOIN operations.
- Translates timestamp strings into timezone-aware UTC datetime objects.
- Handles errors by coercing metadata_amount to a numeric type.

3. Analytics (define_analytics)
- Aggregates the clean data to produce the following summary tables:
    - The total number of events per day for each event type.
    - The total number of unique active users.
    - The user who produced the most events.

## Assumptions
- Input: A single file, raw_events.json, containing a list of event objects.
- Required Fields: user_id, timestamp, and event_type must be present. Events without them are skipped.
- Timestamps: Assumed to be in ISO 8601 format and are converted to UTC.
- Metadata: Expected as a flat key-value object.
- Output: All clean data and aggregations are saved as Parquet files.
-  first user after sorting by user_id.
## How to Run the Pipeline and View the Output

### Prerequisites

* Python 3.8+
* `pandas` library
* `pyarrow` library (for Parquet support)

You can install the required libraries using pip:
```bash
pip install pandas pyarrow
````

### Directory Structure

Ensure your project structure is similar to this:

```
your_project_root/
├── raw_data/
│   └── raw_events.json
├── code/
│   ├── datapipeline.py
│   ├── test_pipeline.py
│   └── outputs.py  # New file for inspection
└── output/                  # This directory will be created by the pipeline
```

Place the provided `raw_events.json` inside the `raw_data` directory.

### Running the Pipeline

1.  Navigate to the `code/` directory in your terminal:
    ```bash
    cd your_project_root/code
    ```
2.  Run the `datapipeline.py` script:
    ```bash
    python datapipeline.py
    ```

### Viewing the Output

After successful execution, the `output/` directory (created at the project root) will contain the following Parquet files:

  * `cleaned_events.parquet`: The fully transformed and cleaned event data.
  * `daily_event_counts.parquet`: A summary of event counts per event type per day.
  * `total_active_users.parquet`: The total count of unique active users.
  * `most_active_user.parquet`: The user ID and event count for the most active app user.
  * `malformed_events.log`: A log file detailing any malformed events encountered during extraction.

To easily inspect the contents of these Parquet files, I've provided a helper script `outputs.py`.

**How to Run the Inspection Script:**

1.  Save the code above as `outputs.py` inside your `code/` directory.
2.  Ensure you have already run `datapipeline.py` at least once to generate the output files.
3.  Navigate to the `code/` directory in your terminal:
    ```bash
    cd your_project_root/code
    ```
4.  Run the inspection script:
    ```bash
    python outputs.py
    ```
    This will print the head of your cleaned data and the full contents of your analytics summary tables directly to your terminal.

### Running Unit Tests

I've developed a dedicated unit test file, `test_pipeline.py`, to ensure the reliability and correctness of the pipeline's individual components. These tests cover essential scenarios for each function:

  * **`extract_data` Tests:** Confirm correct data loading and robust handling of various bad inputs (e.g., missing fields, malformed JSON, non-existent files).
  * **`transform_data` Tests:** Verify data cleaning, including flattening nested metadata, converting timestamps to UTC, and handling numeric conversions and empty inputs.
  * **`define_analytics` Tests:** Check the accuracy of calculations for daily event counts, total active users, and identifying the most active user, including how it behaves with empty datasets.

To run these tests:

1.  Navigate to the `code/` directory in your terminal:
    ```bash
    cd your_project_root/code
    ```
2.  Run the `test_pipeline.py` script:
    ```bash
    python test_pipeline.py
    ```
    You should see output indicating that all tests passed (`OK`).
