# Data Pipeline
This pipeline processes raw JSON user event logs into cleaned, analytics-ready Parquet files. It is also tasked with producing summary aggregations of the data.

## Pipeline Steps
The pipeline is driven by a main function that executes the following modules:

1. **Extract (extract_data)**
- Reads the raw JSON file of events into memory.
- Checks that user_id, event_type, and timestamp are present and non-empty.
- Logs and discards malformed events to allow the pipeline to continue running.

2. **Transform (transform_data)**
- Converts valid records into a Pandas DataFrame.
- Flattens the nested metadata field into top-level columns, eliminating the need for JOIN operations.
- Translates timestamp strings into timezone-aware UTC datetime objects.
- Handles errors by coercing metadata_amount to a numeric type.

3. **Analytics (define_analytics)**
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
└── output/                  # Pipelines create this directory
```

Put the supplied raw_events.json inside the raw_data directory.

### Running the Pipeline

1.  Go to the code/ directory in your terminal:
    ```bash
    cd your_project_root/code
    ```
2.  Run the `datapipeline.py` script:
    ```bash
    python datapipeline.py
    ```

### Viewing the Output
At the conclusion of the successful implementation, the `output/` directory (constructed at the project root) will include the following Parquet files:
  * `cleaned_events.parquet`: Your fully transformed and cleaned event data.
  * `daily_event_counts.parquet`:A summary of event counts per event type per day.
  * `total_active_users.parquet`: The total count of unique active users.
  * `most_active_user.parquet`: The user ID and event count for the most active app user.(commented in datapipeline.py)
  * `malformed_events.log`:A log file that describes any malformed events found in the extraction.

To see the contents of these Parquet files with ease, I have created a man-page script `outputs.py`.

**How to Run the Inspection Script:**

1.  Copy the code above and save it with the name `outputs.py` inside your `code/` directory.
2.  You must have produced the output files by running `datapipeline.py` at least once.
3.  Navigate to your terminal's `code/` directory:
    ```bash
    cd your_project_root/code
    ```
4.  Run the inspection script:
    ```bash
    python outputs.py
    ```
    This will print the head of your cleaned data and the full contents of your analytics summary tables directly to your terminal.

### Running Unit Tests

A specialized unit test file, `test_pipeline.py`, has been created to validate the integrity and correctness of the pipeline's activities. Those tests cover important cases for each feature:

  * **`extract_data` Tests:** Confirm the proper loading of data and the robust handling of many poor entry points (missing fields, bogus JSON, non-existent files).
  * **`transform_data` Tests:** Verify the cleaning of data, including flattening nested metadata, converting timestamps to UTC, managing numbers and empty entries.
  * **`define_analytics` Tests:** Examine the precision of calculations for daily event counts, overall active users, and the most active user, plus how it behaves with blank data sets.

To run these tests:

1.  In your terminal, go to the `code/` directory:
    ```bash
    cd your_project_root/code
    ```
2.  Run the `test_pipeline.py` script:
    ```bash
    python test_pipeline.py
    ```
  You should see output that reports all tests have passed (OK).
