import pandas as pd
import os

def view_pipeline_output():
    """
    Reads and shows the Parquet files created by the pipeline.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, '..', 'output')
        output_dir = os.path.normpath(output_dir)

        print(f"Reading data from: {output_dir}\n")

        cleaned_file = os.path.join(output_dir, 'cleaned_events.parquet')
        daily_counts_file = os.path.join(output_dir, 'daily_event_counts.parquet')
        total_users_file = os.path.join(output_dir, 'total_active_users.parquet')
        most_active_file = os.path.join(output_dir, 'most_active_user.parquet')

        if os.path.exists(cleaned_file):
            cleaned_df = pd.read_parquet(cleaned_file)
            print("Cleaned Events DataFrame (first 10 rows):")
            print(cleaned_df.head(10))
        else:
            print(f"ERROR: File not found at {cleaned_file}")

        if os.path.exists(daily_counts_file):
            daily_counts_df = pd.read_parquet(daily_counts_file)
            print("\nDaily Event Counts DataFrame:")
            print(daily_counts_df)
        else:
            print(f"ERROR: File not found at {daily_counts_file}")

        if os.path.exists(total_users_file):
            total_users_df = pd.read_parquet(total_users_file)
            print("\nTotal Active Users DataFrame:")
            print(total_users_df)
        else:
            print(f"ERROR: File not found at {total_users_file}")

        if os.path.exists(most_active_file):
            most_active_df = pd.read_parquet(most_active_file)
            print("\nMost Active User DataFrame:")
            print(most_active_df)
        else:
            print(f"ERROR: File not found at {most_active_file}")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        print("Make sure that you have run the main datapipeline.py script first to create the output files.")

if __name__ == '__main__':
    view_pipeline_output()
