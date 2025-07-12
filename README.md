# Health Sensing Data Pipeline

This repository contains a simple data pipeline designed to process raw user event logs from a website into a cleaned, structured, and analytics-ready format. The pipeline extracts, transforms, and loads the data, then performs basic aggregations to provide insights into user activity.

## My Approach

I've structured the pipeline into distinct, modular functions: `extract_data`, `transform_data`, and `define_analytics`, orchestrated by a `main` function.

1.  **Data Extraction (`extract_data`):** This function is responsible for reading the raw JSON event logs. It includes robust validation to ensure each event contains the `user_id`, `timestamp`, and `event_type` fields, and that these essential fields are not empty. Any malformed events are logged for review and gracefully skipped to prevent pipeline failures.
2.  **Data Transformation (`transform_data`):** Once extracted, the valid raw events are transformed into a clean, flat tabular format using Pandas DataFrames. A key step here is normalizing the nested `metadata` field into separate columns (e.g., `metadata_screen`, `metadata_amount`). I chose to **flatten the `metadata` field directly into the main DataFrame** rather than separating it into a distinct table for several reasons:
    * **Simplicity for Analytics:** For the current scope of simple aggregations and general use by analytical teams, having all relevant event attributes (including metadata) directly accessible in a single row simplifies queries and avoids complex JOIN operations. This provides a more immediate "wide" view of each event.
    * **Performance for Small Data:** For the expected volume of event data, the overhead of managing multiple tables and performing joins would likely outweigh the benefits of strict normalization. Flattening keeps operations within a single DataFrame, which is efficient for Pandas.
    * **Ease of Use:** Data scientists and analysts often prefer a denormalized view for reporting and initial exploration, as it requires less understanding of underlying database schemas.
    Additionally, I ensured that `timestamp` fields are converted to datetime objects in UTC for consistent time-series analysis and that `metadata_amount` is converted to a numeric type, handling any parsing errors by coercing them to `NaN`.
3.  **Data Aggregation (`define_analytics`):** After transformation, I implemented functions to perform three core analytics tasks:
    * Calculating the total number of events per event type per day.
    * Determining the total number of active users across the dataset.(unique)
    * Identifying the most active user based on their total event count.
    Each aggregation produces a separate, well-structured summary table.

My goal was to create a pipeline that is both functional and maintainable, with clear separation of concerns for each processing step.

## Assumptions Made

* **Input Data Format:** I assumed the `raw_events.json` file is a list of JSON objects, where each object represents a single event.
* **Required Fields:** The fields `user_id`, `timestamp`, and `event_type` are considered mandatory for every event. Events missing these fields or having empty values for them are deemed malformed and are logged and skipped.
* **Timestamp Format:** I assumed `timestamp` strings are primarily in ISO 8601 format (e.g., `YYYY-MM-DDTHH:MM:SS+HH:MM` or `YYYY-MM-DDTHH:MM:SSZ`), allowing for robust parsing with `pd.to_datetime` using `ISO8601` and `utc=True`.
* **Metadata Structure:** The `metadata` field is assumed to be a flat JSON object (key-value pairs) if present, allowing for straightforward flattening using `pd.json_normalize`.
* **Numeric `metadata_amount`:** If `metadata_amount` is present, it's expected to be a string representing a number which can be converted to a float. Non-numeric values are converted to `NaN`.
* **Output Format:** All final, cleaned data and aggregation results are saved in Parquet format, which is efficient for columnar storage and downstream analytical tools.
* **Most Active User Tie-breaking:** If multiple users have the same maximum event count, the pipeline arbitrarily selects one of them (specifically, the one that appears first after sorting by user\_id for consistency in the output, taking `head(1)`).

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