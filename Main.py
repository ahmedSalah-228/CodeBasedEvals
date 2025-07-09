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
# VIEW_NAME = "Sales CC"   # This file processes Sales Department view
# VIEW_NAME="Doctors"
# VIEW_NAME="Delighters"
# VIEW_NAME="Applicants"
VIEW_NAME="Sales MV"
CSV_PATH = f"{VIEW_NAME}.csv"  # Dynamic CSV path based on view name

# Skill filter - change this to filter by different skills
# SKILL_FILTER = "gpt_delighters"
SKILL_FILTER = "gpt_mv_prospect"  # Case-insensitive skill filter
BOT_FILTER = "bot"  # Case-insensitive bot filter
# DEPARTMENT = "Doctors"
# DEPARTMENT = "Delighters"
# DEPARTMENT = "CC Sales"
DEPARTMENT = "Sales MV"


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
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    day_month_year = datetime.now().strftime("%Y-%m-%d")

    df = preprocess_data(df, output_filename=f'{DEPARTMENT}_{day_month_year}.csv')
    
    # Calculate response times
    FRT_df_Raw = calculate_first_response_times(df)
    FRT_df_Raw.to_csv(f"FRT_Raw_{DEPARTMENT}_{day_month_year}.csv", index=False)
    
    non_initial_response_times = calculate_subsequent_response_times(df)
    non_initial_response_times.to_csv(f"non_initial_response_times_{DEPARTMENT}_{day_month_year}.csv", index=False)
    
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
        department=DEPARTMENT
    )
    
    # Compute repetition metrics and push to master CSV
    repetition_columns_to_edit = ['% of Repetition', 'Chats with repetitions', 'Total chats with bot interactions']
    
    repetition_metrics = compute_and_push_metrics_Repetitions(
        df=df,
        master_csv_path=master_csv_path,
        columns_to_edit=repetition_columns_to_edit,
        skill_filter=SKILL_FILTER,
        department=DEPARTMENT
    )
    
    # Compute bot handle metrics and push to master CSV
    bot_handle_columns_to_edit = ['Total chats', 'Conversations Fully Handeled by Bot', 'Bot Handle Ratio']
    
    bot_handle_metrics = compute_and_push_metrics_BotHandle(
        df=df,
        master_csv_path=master_csv_path,
        columns_to_edit=bot_handle_columns_to_edit,
        skill_filter=SKILL_FILTER,
        department=DEPARTMENT
    )
