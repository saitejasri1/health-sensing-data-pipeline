import json 
import pandas as pd
import os
import logging 

# Configuration for file paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_FILE = os.path.join(SCRIPT_DIR, '..', 'raw_data', 'raw_events.json')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, '..', 'output')
LOG_FILE = os.path.join(OUTPUT_DIR, 'malformed_events.log')

# parquet extension files to store cleaned data and analytics results in OUTPUT_DIR
CLEANED_DATA_FILE = os.path.join(OUTPUT_DIR, 'cleaned_events.parquet')
DAILY_EVENT_COUNTS_FILE = os.path.join(OUTPUT_DIR, 'daily_event_counts.parquet')
TOTAL_ACTIVE_USERS_FILE = os.path.join(OUTPUT_DIR, 'total_active_users.parquet')
MOST_ACTIVE_USER_FILE = os.path.join(OUTPUT_DIR, 'most_active_user.parquet')


# Extracting data with metadata in it from the JSON file

def extract_data(filepath):
    """
    Extracts data and metadata from a JSON file and validates the structure of each event.
    """
    valid_data = []
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)

       # Validates required fields ( user_id, timestamp, event_type) and non empty values

        for event in data:
            if not all(k in event for k in ['user_id', 'timestamp', 'event_type']):
                logging.error(f"Malformed event (missing required key): {event}")
                continue
            if not event['user_id'] or not event['timestamp'] or not event['event_type']:
                logging.error(f"Malformed event (empty required value): {event}")
                continue
            valid_data.append(event)
        return valid_data

    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"CRITICAL ERROR: Could not read or parse the input file: {e}")
        return None


def transform_data(events):
    if not events:
        return pd.DataFrame()
    # Normalizing the nested 'metadata' field to flatten the structure and convert datetime strings to datetime objects
    
    df = pd.json_normalize(events, sep='_')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601',utc=True, errors='coerce')
    

    # Converting metadata fields to appropriate data types
    if 'metadata_amount' in df.columns:
        df['metadata_amount'] = pd.to_numeric(df['metadata_amount'], errors='coerce')
      

    df.dropna(subset=['timestamp'], inplace=True)
    return df

def define_analytics(df):
    if df.empty:
        return pd.DataFrame(columns=['date', 'event_type', 'event_count']), \
               pd.DataFrame({'total_active_users': [0]}), \
               pd.DataFrame(columns=['user_id', 'event_count'])
    
    # 1. Computing the total number of events per event type per day.
    df['event_date'] = df['timestamp'].dt.date
    daily_event_counts = df.groupby(['event_date', 'event_type']).size().reset_index(name='event_count')
    daily_event_counts.sort_values(by=['event_date', 'event_count'], inplace=True, ascending=[True, False])

    # 2. Finding the total number of active users.

    # a. Active users defined as users with activity on any day
    active_users = df['user_id'].nunique()
    total_active_users = pd.DataFrame({'total_active_users': [active_users]})

        #b. Active users defined as users with activity on >1 day
    # active_users_per_day = df.groupby('user_id')['event_date'].nunique().reset_index()
    # active_users_per_day = active_users_per_day[active_users_per_day['event_date'] > 1]
    # active_users_count = active_users_per_day['user_id'].nunique()
    # active_users_count_df = pd.DataFrame({'active_users_count': [active_users_count]})

    # 3. Finding the most active app user (user with the most events).
    user_activity = df['user_id'].value_counts().reset_index()
    user_activity.columns = ['user_id', 'event_count']
    
    if user_activity.empty:
        most_active_user_df = pd.DataFrame(columns=['user_id', 'event_count'])
    else:
        max_event_count = user_activity['event_count'].max()
        most_active_user_df = user_activity[user_activity['event_count'] == max_event_count]
        most_active_user_df = most_active_user_df.sort_values('user_id').reset_index(drop=True)
        most_active_user_df = most_active_user_df.head(1) # Take only one if multiple are equally most active


    return daily_event_counts, total_active_users, most_active_user_df

def main(input_path=DEFAULT_INPUT_FILE):
    """
    Main function to run the data pipeline.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    logging.basicConfig(
        filename = LOG_FILE,
        level = logging.ERROR,
        format = '%(asctime)s - %(message)s'
)
    print("Reading and validating events")
    valid_events = extract_data(input_path)
    if not valid_events:
        print("No valid events found. Exiting.")
        return

    print("Transforming data")
    cleaned_df = transform_data(valid_events)
  
    cleaned_df.to_parquet(CLEANED_DATA_FILE, index=False)
    print(f"Successfully wrote cleaned data to {CLEANED_DATA_FILE}")

    print("Running analytics")
    events_per_day, total_active_users, most_active_user = define_analytics(cleaned_df)
    events_per_day.to_parquet(DAILY_EVENT_COUNTS_FILE, index=False)
    total_active_users.to_parquet(TOTAL_ACTIVE_USERS_FILE, index=False)
    most_active_user.to_parquet(MOST_ACTIVE_USER_FILE, index=False)
    print(f"Analytics results saved in {OUTPUT_DIR}")

    print("\nPipeline deployed!")

if __name__ == '__main__':
    main()
