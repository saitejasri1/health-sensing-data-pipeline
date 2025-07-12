import pandas as pd

cleaned_df = pd.read_parquet('output/cleaned_events.parquet')
print("\nCleaned Events DataFrame:")
print(cleaned_df.head(10))

daily_counts_df = pd.read_parquet('output/daily_event_counts.parquet')
print("\nDaily Event Counts DataFrame:")
print(daily_counts_df)

total_users_df = pd.read_parquet('output/total_active_users.parquet')
print("\nTotal Active Users DataFrame:")
print(total_users_df)

most_active_df = pd.read_parquet('output/most_active_user.parquet')
print("\nMost Active User DataFrame:")
print(most_active_df)