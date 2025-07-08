import pandas as pd
from Utilities_2 import (
    calculate_first_response_times, 
    calculate_subsequent_response_times,
    compute_and_push_metrics,
    compute_and_push_metrics_Repetitions,
    compute_and_push_metrics_BotHandle,
    preprocess_data,
    count_conversations_with_skills
)
from fetch import fetch_data
from datetime import datetime

# Configuration - this file is specifically for Sales Department data
VIEW_NAME = "Applicants"   # This file processes Sales Department view
CSV_PATH = f"{VIEW_NAME}.csv"  # Dynamic CSV path based on view name

# Skill filter - change this to filter by different skills
SKILL_FILTER = "filipina_outside"  # Case-insensitive skill filter
BOT_FILTER = "bot"  # Case-insensitive bot filter

# Main execution
if __name__ == "__main__":
    # Load and preprocess data
    # Fetch data from Tableau
    print(f"Fetching data for view: {VIEW_NAME}")
    if fetch_data(VIEW_NAME, CSV_PATH):
        print("Data fetched successfully from Tableau")
    else:
        print("Failed to fetch data from Tableau")
        exit(1)
    df=pd.read_csv(CSV_PATH)
    df = preprocess_data(df, output_filename='Sales_2_July_sorted.csv')
    
    # Calculate response times
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    FRT_df_Raw = calculate_first_response_times(df)
    FRT_df_Raw.to_csv(f"FRT_Raw_sales_{current_datetime}.csv", index=False)
    
    non_initial_response_times = calculate_subsequent_response_times(df)
    non_initial_response_times.to_csv(f"non_initial_response_times_{SKILL_FILTER}_{current_datetime}.csv", index=False)
    
    # Compute metrics and push to master CSV
    master_csv_path = "Master Sheet.csv"  # Change this to your actual master CSV path
    
    # Specify which columns to edit (optional - if None, all metrics will be added)
    columns_to_edit = ['AVG initial', 'AVG non_initial', 'Count of >=4 mins initial', 'Count of >=4 mins non_initial']
    
    metrics = compute_and_push_metrics(
        frt_df=FRT_df_Raw,
        non_initial_df=non_initial_response_times,
        master_csv_path=master_csv_path,
        columns_to_edit=columns_to_edit,
        skill_filter=SKILL_FILTER,
        bot_filter=BOT_FILTER,
        department="AT Filipina_Outside"
    )
    
    # Compute repetition metrics and push to master CSV
    repetition_columns_to_edit = ['% of Repetition', 'Chats with repetitions', 'Total chats with bot interactions']
    
    repetition_metrics = compute_and_push_metrics_Repetitions(
        df=df,
        master_csv_path=master_csv_path,
        columns_to_edit=repetition_columns_to_edit,
        skill_filter=SKILL_FILTER,
        department="AT Filipina_Outside"
    )
    
    # Compute bot handle metrics and push to master CSV
    bot_handle_columns_to_edit = ['Total chats', 'Conversations Fully Handeled by Bot', 'Bot Handle Ratio']
    
    bot_handle_metrics = compute_and_push_metrics_BotHandle(
        df=df,
        master_csv_path=master_csv_path,
        columns_to_edit=bot_handle_columns_to_edit,
        skill_filter=SKILL_FILTER,
        department="AT Filipina_Outside"
    )