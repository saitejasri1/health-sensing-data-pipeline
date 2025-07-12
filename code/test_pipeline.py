import unittest
import pandas as pd
import json
import os
from datetime import datetime, date
from pathlib import Path
import logging 
import logging.handlers # IMPORTANT: Needed for robust logging setup

# Importing data pipeline functions
from datapipeline import (
    extract_data,
    transform_data,
    define_analytics,
    OUTPUT_DIR,
    LOG_FILE, # This refers to the constant from datapipeline.py
    CLEANED_DATA_FILE,
    DAILY_EVENT_COUNTS_FILE,
    TOTAL_ACTIVE_USERS_FILE,
    MOST_ACTIVE_USER_FILE
)


class TestDataPipeline(unittest.TestCase):
    """
    Overview of Test Cases for the Data Pipeline:

    This test suite validates the core functionalities of the data pipeline:
    - Data Extraction: Ensures correct loading of JSON data, handling of malformed
      events (missing keys, empty values), and graceful failure for invalid files.
    - Data Transformation: Verifies that raw event data is correctly normalized,
      timestamps are converted, and metadata fields are appropriately typed.
      Includes tests for empty input and malformed specific data points.
    - Data Analytics: Confirms that aggregation functions produce accurate
      summary tables for daily event counts, total active users, and the most
      active user, covering both typical and empty data scenarios.
    """

    def setUp(self):
        global LOG_FILE # Declare global before first use

        self.test_output_dir = "test_output"
        os.makedirs(self.test_output_dir, exist_ok=True)

        self.temp_input_json = Path(self.test_output_dir) / "temp_raw_events.json"
        self.temp_log_file = Path(self.test_output_dir) / "temp_malformed_events.log"
        
        self.original_log_file_path = LOG_FILE # Store original global path

        # Store original root logger state to restore it later
        self._original_handlers = logging.getLogger().handlers[:]
        self._original_level = logging.getLogger().level
        self._original_propagate = logging.getLogger().propagate

        LOG_FILE = str(self.temp_log_file) # Set global LOG_FILE to temp path for tests
        
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.ERROR)
        
        # Remove all existing handlers from the root logger before adding test-specific one
        for handler in self.logger.handlers[:]: 
            self.logger.removeHandler(handler)

        # Create a FileHandler for the test log file
        self.file_handler = logging.FileHandler(self.temp_log_file, mode='w')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)
        self.logger.propagate = False # Prevent logs from showing on console during tests


    def tearDown(self):
        global LOG_FILE # Declare global before first use

        if os.path.exists(self.test_output_dir):
            for f in os.listdir(self.test_output_dir):
                os.remove(os.path.join(self.test_output_dir, f))
            os.rmdir(self.test_output_dir)

        # Clean up logging handlers properly
        if hasattr(self, 'file_handler') and self.file_handler:
            self.file_handler.close() # Close the file handler opened in setUp
            self.logger.removeHandler(self.file_handler) # Remove the handler

        # Restore original logging configuration
        self.logger.propagate = self._original_propagate
        self.logger.setLevel(self._original_level)
        for handler in self._original_handlers:
            self.logger.addHandler(handler)

        LOG_FILE = self.original_log_file_path # Restore original LOG_FILE path


    #  Test Cases for extract_data function 
    def test_extract_data_valid_json(self):
        test_data = [
            {"user_id": "u1", "timestamp": "2025-01-01T10:00:00Z", "event_type": "click", "metadata": {"screen": "home"}},
            {"user_id": "u2", "timestamp": "2025-01-01T11:00:00Z", "event_type": "purchase", "metadata": {"amount": "10.00"}}
        ]
        with open(self.temp_input_json, 'w') as f:
            json.dump(test_data, f)

        extracted_events = extract_data(self.temp_input_json)
        self.assertIsInstance(extracted_events, list)
        self.assertEqual(len(extracted_events), 2)
        self.assertEqual(extracted_events[0]['user_id'], 'u1')
        self.assertEqual(extracted_events[1]['event_type'], 'purchase')

    def test_extract_data_missing_required_key(self):
        test_data = [
            {"user_id": "u1", "timestamp": "2025-01-01T10:00:00Z", "metadata": {"screen": "home"}}
        ]
        with open(self.temp_input_json, 'w') as f:
            json.dump(test_data, f)

        extracted_events = extract_data(self.temp_input_json)
        self.assertEqual(len(extracted_events), 0)


    def test_extract_data_empty_required_value(self):
        test_data = [
            {"user_id": "", "timestamp": "2025-01-01T10:00:00Z", "event_type": "click", "metadata": {"screen": "home"}}
        ]
        with open(self.temp_input_json, 'w') as f:
            json.dump(test_data, f)

        extracted_events = extract_data(self.temp_input_json)
        self.assertEqual(len(extracted_events), 0)

    def test_extract_data_file_not_found(self):
        extracted_events = extract_data(Path("non_existent_file.json"))
        self.assertIsNone(extracted_events)

    def test_extract_data_malformed_json(self):
        with open(self.temp_input_json, 'w') as f:
            f.write("{'user_id': 'u1', 'timestamp': '2025-01-01T10:00:00Z', 'event_type': 'click'")

        extracted_events = extract_data(self.temp_input_json)
        self.assertIsNone(extracted_events)


    #  Test Cases for transform_data function 
    def test_transform_data_basic_conversion(self):
        events = [
            {"user_id": "u1", "timestamp": "2025-01-01T10:00:00Z", "event_type": "click", "metadata": {"screen": "home"}},
            {"user_id": "u2", "timestamp": "2025-01-01T11:00:00Z", "event_type": "purchase", "metadata": {"amount": "10.50", "currency": "USD"}}
        ]
        df = transform_data(events)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

        self.assertIn('user_id', df.columns)
        self.assertIn('timestamp', df.columns)
        self.assertIn('event_type', df.columns)
        self.assertIn('metadata_screen', df.columns)
        self.assertIn('metadata_amount', df.columns)

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['timestamp']))
        self.assertTrue(pd.api.types.is_float_dtype(df['metadata_amount']))

        self.assertEqual(df.loc[0, 'user_id'], 'u1')
        self.assertEqual(df.loc[0, 'timestamp'], datetime(2025, 1, 1, 10, 0, 0, tzinfo=pd.Timestamp('2025-01-01 10:00:00+00:00').tz))
        self.assertEqual(df.loc[1, 'metadata_amount'], 10.50)

    def test_transform_data_empty_input(self):
        df = transform_data([])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_transform_data_malformed_timestamp_and_amount(self):
        events = [
            {"user_id": "u1", "timestamp": "invalid-date", "event_type": "click", "metadata": {"screen": "home"}},
            {"user_id": "u2", "timestamp": "2025-01-01T11:00:00Z", "event_type": "purchase", "metadata": {"amount": "not_a_number", "currency": "USD"}}
        ]
        df = transform_data(events)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.loc[df.index[0], 'user_id'], 'u2')
        self.assertTrue(pd.isna(df.loc[df.index[0], 'metadata_amount']))

    #  Test Cases for define_analytics function 
    def test_define_analytics_basic(self):
        sample_df = pd.DataFrame([
            {'user_id': 'A', 'timestamp': datetime(2025, 3, 1, 10, 0, 0), 'event_type': 'click', 'metadata_screen': 'home'},
            {'user_id': 'B', 'timestamp': datetime(2025, 3, 1, 11, 0, 0), 'event_type': 'purchase', 'metadata_amount': 50.0},
            {'user_id': 'A', 'timestamp': datetime(2025, 3, 1, 12, 0, 0), 'event_type': 'click', 'metadata_screen': 'product'},
            {'user_id': 'C', 'timestamp': datetime(2025, 3, 2, 10, 0, 0), 'event_type': 'view', 'metadata_screen': 'about'},
            {'user_id': 'A', 'timestamp': datetime(2025, 3, 2, 11, 0, 0), 'event_type': 'click', 'metadata_screen': 'home'},
        ])
        sample_df['timestamp'] = pd.to_datetime(sample_df['timestamp'])

        daily_counts, active_users, most_active = define_analytics(sample_df)

        self.assertIsInstance(daily_counts, pd.DataFrame)
        self.assertEqual(len(daily_counts), 4)
        self.assertEqual(daily_counts.loc[(daily_counts['event_date'] == date(2025, 3, 1)) & (daily_counts['event_type'] == 'click'), 'event_count'].iloc[0], 2)
        self.assertEqual(daily_counts.loc[(daily_counts['event_date'] == date(2025, 3, 1)) & (daily_counts['event_type'] == 'purchase'), 'event_count'].iloc[0], 1)
        self.assertEqual(daily_counts.loc[(daily_counts['event_date'] == date(2025, 3, 2)) & (daily_counts['event_type'] == 'view'), 'event_count'].iloc[0], 1)
        self.assertEqual(daily_counts.loc[(daily_counts['event_date'] == date(2025, 3, 2)) & (daily_counts['event_type'] == 'click'), 'event_count'].iloc[0], 1)

        self.assertIsInstance(active_users, pd.DataFrame)
        self.assertEqual(active_users['total_active_users'].iloc[0], 3)

        self.assertIsInstance(most_active, pd.DataFrame)
        self.assertEqual(most_active['user_id'].iloc[0], 'A')
        self.assertEqual(most_active['event_count'].iloc[0], 3)

    def test_define_analytics_empty_input(self):
        empty_df = pd.DataFrame()
        daily_counts, active_users, most_active = define_analytics(empty_df)

        self.assertTrue(daily_counts.empty)
        self.assertEqual(active_users['total_active_users'].iloc[0], 0) 
        self.assertTrue(most_active.empty)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)