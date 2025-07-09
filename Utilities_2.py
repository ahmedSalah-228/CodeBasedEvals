import pandas as pd
from datetime import datetime

def preprocess_data(df, output_filename='MV_2_July_sorted.csv'):
    #sort by conversation id and message sent time
    df=df.sort_values(by=['Conversation ID', 'Message Sent Time'])
    #drop duplicates
    df=df.drop_duplicates(subset=['Conversation ID', 'Message Sent Time'],keep='first')
    df.to_csv(output_filename,index=False)
    return df

def calculate_first_response_times(df):
    df['Message Sent Time'] = pd.to_datetime(df['Message Sent Time'])
    response_times = []

    for conv_id, group in df.groupby("Conversation ID"):
        group = group[
    (group['Message Type'].str.lower().isin(['normal message', 'transfer'])) |
    ((group['Message Type'].str.lower() == 'private message') & (group['Sent By'].str.lower() == 'system'))
].copy()
        first_consumer_message_time = None
        first_response_recorded = False  # Flag to track if first response is recorded
        for i in range(len(group)):
            current_row = group.iloc[i]
            sender = str(current_row["Sent By"]).strip().lower()
            message_type = str(current_row["Message Type"]).strip().lower()  # Get message type
            skill=str(current_row["Skill"]).strip().lower()


            if sender == "consumer" and first_consumer_message_time is None:
                first_consumer_message_time = current_row["Message Sent Time"]

            # Check for "Transfer" message before the first response
            elif message_type == "transfer" and first_consumer_message_time is not None:
                first_consumer_message_time = current_row["Message Sent Time"]  # Update consumer message time
            # Case c: System private message resets consumer time
            elif sender == "system" and message_type == 'private message' and first_consumer_message_time is not None:
               first_consumer_message_time = current_row['Message Sent Time']

            elif sender in ["bot", "agent","system"] and first_consumer_message_time is not None and not first_response_recorded:
                time_diff = (current_row["Message Sent Time"] - first_consumer_message_time).total_seconds()
                time_diff = round(time_diff / 60, 2)

                if sender == "bot":
                    sender_name = "BOT" + "_" +skill
                elif sender == "agent":
                    sender_name = current_row["Agent Name "] + "_" + skill
                else:
                    sender_name = "System"

                if sender_name:
                    response_times.append({
                        "Conversation Id": conv_id,
                        "Sender": sender_name,
                        "Response Time (mins)": time_diff,
                        "Message Id": current_row["MESSAGE_ID"]
                    })
                    first_response_recorded = True  # Set the flag to True once the first response is recorded
                    break

    return pd.DataFrame(response_times)

#Here we calculate response times except the first response time

def calculate_subsequent_response_times(df):
    df['Message Sent Time'] = pd.to_datetime(df['Message Sent Time'])
    response_times = []

    for conv_id, group in df.groupby("Conversation ID"):
        group = group[
    (group['Message Type'].str.lower().isin(['normal message', 'transfer'])) |
    ((group['Message Type'].str.lower() == 'private message') & (group['Sent By'].str.lower() == 'system'))
].copy()
        first_consumer_message_time = None
        first_response_recorded = False  # Flag to track if first response is recorded

        for i in range(len(group)):
            current_row = group.iloc[i]
            sender = str(current_row["Sent By"]).strip().lower()
            message_type = str(current_row["Message Type"]).strip().lower()  # Get message type
            skill=str(current_row["Skill"]).strip().lower()

            if sender == "consumer" and first_consumer_message_time is None:  # Only update if it's the first in a sequence

                first_consumer_message_time = current_row["Message Sent Time"]

            # Check for "Transfer" message before the first response
            elif message_type == "transfer" and first_consumer_message_time is not None:
                first_consumer_message_time = current_row["Message Sent Time"]  # Update consumer message time when transfers happens

            # Case c: System private message resets consumer time
            elif sender == "system" and message_type == 'private message' and first_consumer_message_time is not None:
               first_consumer_message_time = current_row['Message Sent Time']

            elif sender in ["bot", "agent","system"] and first_consumer_message_time is not None:
                if not first_response_recorded:
                    first_response_recorded = True  # Skip the first response
                    first_consumer_message_time = None
                    continue  # Move to the next message

                time_diff = (current_row["Message Sent Time"] - first_consumer_message_time).total_seconds()
                time_diff = round(time_diff / 60, 2)

                if sender == "bot":
                    sender_name = "BOT" + "_" +skill
                elif sender == "agent":
                    sender_name = current_row["Agent Name "]
                else:
                    sender_name = "System"

                if sender_name:
                    response_times.append({
                        "Conversation Id": conv_id,
                        "Sender": sender_name,
                        "Response Time (mins)": time_diff,
                        "Message Id": current_row["MESSAGE_ID"]
                    })
                    first_consumer_message_time = None

    return pd.DataFrame(response_times)

def compute_and_push_metrics(frt_df, non_initial_df, master_csv_path, columns_to_edit=None, skill_filter="filipina_outside", bot_filter="bot", department="Sales"):
    """
    Compute metrics from FRT and non-initial response times and append to master CSV
    
    Args:
        frt_df: DataFrame with first response times
        non_initial_df: DataFrame with non-initial response times  
        master_csv_path: Path to the master CSV file
        columns_to_edit: List of column names to edit (e.g., ['AVG initial', 'AVG non_initial', 'Count of >=4 mins'])
        skill_filter: Skill to filter by (default: "filipina_outside")
        bot_filter: Bot filter (default: "bot")
        department: Department name (default: "Sales")
    """
    
    # Compute metrics
    metrics = {}
    
    # Average initial response time (only for responses < 4 mins and sender contains the specified skill AND bot)
    if not frt_df.empty and 'Response Time (mins)' in frt_df.columns and 'Sender' in frt_df.columns:
        filtered_frt = frt_df[
            (frt_df['Response Time (mins)'] < 4) & 
            (frt_df['Sender'].str.contains(skill_filter, case=False, na=False)) &
            (frt_df['Sender'].str.contains(bot_filter, case=False, na=False))
        ]
        metrics['AVG initial'] = round(filtered_frt['Response Time (mins)'].mean(), 2) if not filtered_frt.empty else 0
    else:
        metrics['AVG initial'] = 0
    
    # Average non-initial response time (only for responses < 4 mins and sender contains the specified skill AND bot)
    if not non_initial_df.empty and 'Response Time (mins)' in non_initial_df.columns and 'Sender' in non_initial_df.columns:
        filtered_non_initial = non_initial_df[
            (non_initial_df['Response Time (mins)'] < 4) & 
            (non_initial_df['Sender'].str.contains(skill_filter, case=False, na=False)) &
            (non_initial_df['Sender'].str.contains(bot_filter, case=False, na=False))
        ]
        metrics['AVG non_initial'] = round(filtered_non_initial['Response Time (mins)'].mean(), 2) if not filtered_non_initial.empty else 0
    else:
        metrics['AVG non_initial'] = 0
    
    # Count of initial responses >= 4 minutes (with skill filtering)
    count_4plus_initial = 0
    if not frt_df.empty and 'Response Time (mins)' in frt_df.columns and 'Sender' in frt_df.columns:
        filtered_frt_4plus = frt_df[
            (frt_df['Response Time (mins)'] >= 4) & 
            (frt_df['Sender'].str.contains(skill_filter, case=False, na=False)) &
            (frt_df['Sender'].str.contains(bot_filter, case=False, na=False))
        ]
        count_4plus_initial = len(filtered_frt_4plus)
    
    # Count of non-initial responses >= 4 minutes (with skill filtering)
    count_4plus_non_initial = 0
    if not non_initial_df.empty and 'Response Time (mins)' in non_initial_df.columns and 'Sender' in non_initial_df.columns:
        filtered_non_initial_4plus = non_initial_df[
            (non_initial_df['Response Time (mins)'] >= 4) & 
            (non_initial_df['Sender'].str.contains(skill_filter, case=False, na=False)) &
            (non_initial_df['Sender'].str.contains(bot_filter, case=False, na=False))
        ]
        count_4plus_non_initial = len(filtered_non_initial_4plus)
    
    metrics['Count of >=4 mins initial'] = count_4plus_initial
    metrics['Count of >=4 mins non_initial'] = count_4plus_non_initial
    
    print(f"Computed metrics:")
    print(f"  AVG initial: {metrics['AVG initial']} minutes")
    print(f"  AVG non_initial: {metrics['AVG non_initial']} minutes")
    print(f"  Count of >=4 mins initial: {metrics['Count of >=4 mins initial']}")
    print(f"  Count of >=4 mins non_initial: {metrics['Count of >=4 mins non_initial']}")
    
    # Append to master CSV
    try:
        # Try to load existing master CSV
        try:
            master_df = pd.read_csv(master_csv_path)
        except FileNotFoundError:
            # Create new master CSV if it doesn't exist
            master_df = pd.DataFrame()
        
        # Add date in the specified format (e.g., "July 01, 2024")
        current_date = datetime.now()
        month_name = current_date.strftime("%B")  # Full month name
        day = current_date.strftime("%d")  # Day with leading zero
        year = current_date.strftime("%Y")  # Full year
        metrics['Date'] = f"{month_name} {day}, {year}"
        metrics['Department'] = department
        
        # If columns_to_edit is specified, only update those columns
        if columns_to_edit:
            # Create a new row with only the specified columns
            new_row = {}
            for col in columns_to_edit:
                if col in metrics:
                    new_row[col] = metrics[col]
                else:
                    new_row[col] = 0  # Default value if column doesn't exist in metrics
            
            # Add date and department
            new_row['Date'] = metrics['Date']
            new_row['Department'] = metrics['Department']
            
            # Check if row with same department and date already exists
            existing_row_mask = (master_df['Department'] == new_row['Department']) & (master_df['Date'] == new_row['Date'])
            
            if existing_row_mask.any():
                # Always overwrite existing row (regardless of null values)
                existing_row_idx = existing_row_mask.idxmax()
                for col in columns_to_edit:
                    if col in new_row:
                        master_df.at[existing_row_idx, col] = new_row[col]
                print(f"Overwrote existing row for {new_row['Department']} on {new_row['Date']}")
            else:
                # Append new row since it doesn't exist
                master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
                print(f"Added new row for {new_row['Department']} on {new_row['Date']}")
        else:
            # Check if row with same department and date already exists
            existing_row_mask = (master_df['Department'] == metrics['Department']) & (master_df['Date'] == metrics['Date'])
            
            if existing_row_mask.any():
                # Always overwrite existing row (regardless of null values)
                existing_row_idx = existing_row_mask.idxmax()
                for col in metrics.keys():
                    if col not in ['Date', 'Department']:  # Don't overwrite these
                        master_df.at[existing_row_idx, col] = metrics[col]
                print(f"Overwrote existing row for {metrics['Department']} on {metrics['Date']}")
            else:
                # Append new row since it doesn't exist
                master_df = pd.concat([master_df, pd.DataFrame([metrics])], ignore_index=True)
                print(f"Added new row for {metrics['Department']} on {metrics['Date']}")
        
        master_df.to_csv(master_csv_path, index=False)
        print(f"Successfully appended metrics to {master_csv_path}")
        
    except Exception as e:
        print(f"Error appending to master CSV: {e}")
        # Save metrics to separate CSV as fallback
        metrics_df = pd.DataFrame([metrics])
        metrics_df.to_csv(f"metrics_{department.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", index=False)
        print("Saved metrics to separate CSV as fallback")
    
    return metrics

def count_conversations_with_skills(df):
    # Convert skill columns to lowercase for case-insensitive matching
    skill_columns = [col for col in df.columns if 'skill' in col.lower()]
    
    if not skill_columns:
        print("No skill columns found in the dataset")
        return 0
    
    print(f"Found skill columns: {skill_columns}")
    
    # Create a mask for messages with the specified skills
    skill_mask = pd.Series([False] * len(df), index=df.index)
    
    for col in skill_columns:
        # Convert to string and lowercase for case-insensitive matching
        col_values = df[col].astype(str).str.lower()
        
        # Check for the specified skills
        gpt_sales_mask = col_values.str.contains('gpt_cc_prospect', na=False)
        # Add other skill patterns as needed
        
        skill_mask = skill_mask | gpt_sales_mask 
    
    # Get unique conversation IDs that have at least one message with the specified skills
    conversations_with_skills = df[skill_mask]['Conversation ID'].unique()
    
    print(f"Total conversations: {df['Conversation ID'].nunique()}")
    print(f"Conversations with GPT_SALES: {len(conversations_with_skills)}")
    
    return len(conversations_with_skills)

def get_bot_repetitions(df, skill_filter="filipina_outside"):
    """
    Get bot repetition metrics from conversation data
    
    Args:
        df: DataFrame with conversation data
        skill_filter: Skill to filter by (default: "filipina_outside")
    
    Returns:
        tuple: (repetitions_df, percentage, chats_with_reps, total_chats)
    """
    # Create an empty list to store repetition data
    repetition_data = []
    chats_with_repetition = set()
    
    # For every conversation id we want bot repetitions
    for conversation_id in df['Conversation ID'].unique():
        conversation_df = df[df['Conversation ID'] == conversation_id]
        # Get the bot messages
        bot_messages = conversation_df[(conversation_df['Sent By'].str.lower() == 'bot') & 
                                     (conversation_df['Message Type'].str.lower() == 'normal message') &
                                     (conversation_df['Skill'].str.contains(skill_filter, na=False, case=False))]
        # Count occurrences of each message
        message_counts = bot_messages['TEXT'].value_counts()
        
        # Get messages that appear more than once
        repeated_messages = message_counts[message_counts > 1]
        
        # For each repeated message, get its first occurrence and count
        for message, count in repeated_messages.items():
            first_occurrence = bot_messages[bot_messages['TEXT'] == message].iloc[0]
            repetition_data.append({
                'Conversation ID': conversation_id,
                'Message Id': first_occurrence.get('MESSAGE_ID', ''),
                'Message': message,
                'Repetition Count': count
            })
            chats_with_repetition.add(conversation_id)
    
    # Create DataFrame from repetition data
    repetitions_df = None
    if repetition_data:
        repetitions_df = pd.DataFrame(repetition_data)
    else:
        print("No repetitions found")
    
    # Calculate total chats with bot interactions
    total_chats = 0
    for conversation_id in df['Conversation ID'].unique():
        conversation_df = df[df['Conversation ID'] == conversation_id]
        if conversation_df['Skill'].str.contains(skill_filter, case=False).any():
            total_chats += 1
    
    chats_with_reps = len(chats_with_repetition)
    percentage = (chats_with_reps / total_chats) * 100 if total_chats > 0 else 0
    
    print(f"% of chats with at least one repetition: {chats_with_reps} / {total_chats} = {percentage:.2f}%")
    
    return repetitions_df, percentage, chats_with_reps, total_chats

def compute_and_push_metrics_Repetitions(df, master_csv_path, columns_to_edit=None, skill_filter="filipina_outside", department="Sales"):
    """
    Compute repetition metrics and push to master CSV
    
    Args:
        df: DataFrame with conversation data
        master_csv_path: Path to the master CSV file
        columns_to_edit: List of column names to edit (e.g., ['% of Repetition', 'Chats with repetitions', 'Total chats with bot interactions'])
        skill_filter: Skill to filter by (default: "filipina_outside")
        department: Department name (default: "Sales")
    """
    
    # Get repetition metrics
    repetitions_df, percentage, chats_with_reps, total_chats = get_bot_repetitions(df, skill_filter)
    
    # Compute metrics
    metrics = {}
    metrics['% of Repetition'] = round(percentage, 2)
    metrics['Chats with repetitions'] = chats_with_reps
    metrics['Total chats with bot interactions'] = total_chats
    
    print(f"Computed repetition metrics:")
    print(f"  % of Repetition: {metrics['% of Repetition']}%")
    print(f"  Chats with repetitions: {metrics['Chats with repetitions']}")
    print(f"  Total chats with bot interactions: {metrics['Total chats with bot interactions']}")
    
    # Append to master CSV
    try:
        # Try to load existing master CSV
        try:
            master_df = pd.read_csv(master_csv_path)
        except FileNotFoundError:
            # Create new master CSV if it doesn't exist
            master_df = pd.DataFrame()
        
        # Add date in the specified format (e.g., "July 01, 2024")
        current_date = datetime.now()
        month_name = current_date.strftime("%B")  # Full month name
        day = current_date.strftime("%d")  # Day with leading zero
        year = current_date.strftime("%Y")  # Full year
        metrics['Date'] = f"{month_name} {day}, {year}"
        metrics['Department'] = department
        repetitions_df.to_csv(f"repetitions_df_{department.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", index=False)  
        
        # If columns_to_edit is specified, only update those columns
        if columns_to_edit:
            # Create a new row with only the specified columns
            new_row = {}
            for col in columns_to_edit:
                if col in metrics:
                    new_row[col] = metrics[col]
                else:
                    new_row[col] = 0  # Default value if column doesn't exist in metrics
            
            # Add date and department
            new_row['Date'] = metrics['Date']
            new_row['Department'] = metrics['Department']
            
            # Check if row with same department and date already exists
            existing_row_mask = (master_df['Department'] == new_row['Department']) & (master_df['Date'] == new_row['Date'])
            
            if existing_row_mask.any():
                # Always overwrite existing row (regardless of null values)
                existing_row_idx = existing_row_mask.idxmax()
                for col in columns_to_edit:
                    if col in new_row:
                        master_df.at[existing_row_idx, col] = new_row[col]
                print(f"Overwrote existing row for {new_row['Department']} on {new_row['Date']}")
            else:
                # Append new row since it doesn't exist
                master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
                print(f"Added new row for {new_row['Department']} on {new_row['Date']}")
        else:
            # Check if row with same department and date already exists
            existing_row_mask = (master_df['Department'] == metrics['Department']) & (master_df['Date'] == metrics['Date'])
            
            if existing_row_mask.any():
                # Always overwrite existing row (regardless of null values)
                existing_row_idx = existing_row_mask.idxmax()
                for col in metrics.keys():
                    if col not in ['Date', 'Department']:  # Don't overwrite these
                        master_df.at[existing_row_idx, col] = metrics[col]
                print(f"Overwrote existing row for {metrics['Department']} on {metrics['Date']}")
            else:
                # Append new row since it doesn't exist
                master_df = pd.concat([master_df, pd.DataFrame([metrics])], ignore_index=True)
                print(f"Added new row for {metrics['Department']} on {metrics['Date']}")
        
        master_df.to_csv(master_csv_path, index=False)
        print(f"Successfully appended repetition metrics to {master_csv_path}")
        
    except Exception as e:
        print(f"Error appending to master CSV: {e}")
        # Save metrics to separate CSV as fallback
        metrics_df = pd.DataFrame([metrics])
        metrics_df.to_csv(f"repetition_metrics_{department.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", index=False)
        print("Saved repetition metrics to separate CSV as fallback")
    
    return metrics

def get_bot_handle_metrics(df, skill_filter="gpt_cc_prospect"):
    """
    Get bot handle metrics from conversation data
    
    Args:
        df: DataFrame with conversation data
        skill_filter: Skill to filter by (default: "gpt_cc_prospect")
    
    Returns:
        tuple: (total_chats, fully_bot_conversations, bot_handle_ratio)
    """
    # Convert skill columns to lowercase for case-insensitive matching
    skill_columns = [col for col in df.columns if 'skill' in col.lower()]
    
    if not skill_columns:
        print("No skill columns found in the dataset")
        return 0, 0, 0
    
    print(f"Found skill columns: {skill_columns}")
    
    # Group by conversation ID
    conversation_groups = df.groupby('Conversation ID')
    
    fully_bot_conversations = []
    total_chats_with_skill = 0
    
    for conversation_id, group in conversation_groups:
        # Check if the conversation has the specified skill
        has_skill = False
        for col in skill_columns:
            col_values = group[col].astype(str).str.lower()
            if col_values.str.contains(skill_filter, na=False).any():
                has_skill = True
                break
        
        if not has_skill:
            continue  # Skip conversations without the specified skill
        
        total_chats_with_skill += 1
        
        # Check if there are any Agent interactions based on "Agent Name " column
        has_agent_interaction = group['Agent Name '].notna().any()
        
        if has_agent_interaction:
            continue  # Skip conversations with Agent interaction
        
        # Check if all messages are bot messages
        # Bot messages are identified by having no Agent Name (isna)
        all_bot_messages = group['Agent Name '].isna().all()
        
        if not all_bot_messages:
            continue  # Skip if not all messages are bot messages
        
        # If we reach here, it's a fully bot-handled conversation
        fully_bot_conversations.append(conversation_id)
    
    # Calculate bot handle ratio
    total_chats = df['Conversation ID'].nunique()
    bot_handle_ratio = (len(fully_bot_conversations) / total_chats) * 100 if total_chats > 0 else 0
    
    print(f"Total chats with {skill_filter} skill: {total_chats_with_skill}")
    print(f"Conversations handled fully by bot: {len(fully_bot_conversations)}")
    print(f"Bot Handle Ratio: {bot_handle_ratio:.2f}%")
    
    return total_chats, len(fully_bot_conversations), bot_handle_ratio

def compute_and_push_metrics_BotHandle(df, master_csv_path, columns_to_edit=None, skill_filter="gpt_cc_prospect", department="Sales"):
    """
    Compute bot handle metrics and push to master CSV
    
    Args:
        df: DataFrame with conversation data
        master_csv_path: Path to the master CSV file
        columns_to_edit: List of column names to edit (e.g., ['Total chats', 'Conversations Fully Handeled by Bot', 'Bot Handle Ratio'])
        skill_filter: Skill to filter by (default: "filipina_outside")
        department: Department name (default: "Sales")
    """
    
    # Get bot handle metrics
    total_chats, fully_bot_conversations, bot_handle_ratio = get_bot_handle_metrics(df, skill_filter)
    
    # Compute metrics
    metrics = {}
    metrics['Total chats'] = total_chats
    metrics['Conversations Fully Handeled by Bot'] = fully_bot_conversations
    metrics['Bot Handle Ratio'] = round(bot_handle_ratio, 2)
    
    print(f"Computed bot handle metrics:")
    print(f"  Total chats: {metrics['Total chats']}")
    print(f"  Conversations Fully Handeled by Bot: {metrics['Conversations Fully Handeled by Bot']}")
    print(f"  Bot Handle Ratio: {metrics['Bot Handle Ratio']}%")
    
    # Append to master CSV
    try:
        # Try to load existing master CSV
        try:
            master_df = pd.read_csv(master_csv_path)
        except FileNotFoundError:
            # Create new master CSV if it doesn't exist
            master_df = pd.DataFrame()
        
        # Add date in the specified format (e.g., "July 01, 2024")
        current_date = datetime.now()
        month_name = current_date.strftime("%B")  # Full month name
        day = current_date.strftime("%d")  # Day with leading zero
        year = current_date.strftime("%Y")  # Full year
        metrics['Date'] = f"{month_name} {day}, {year}"
        metrics['Department'] = department
        
        # If columns_to_edit is specified, only update those columns
        if columns_to_edit:
            # Create a new row with only the specified columns
            new_row = {}
            for col in columns_to_edit:
                if col in metrics:
                    new_row[col] = metrics[col]
                else:
                    new_row[col] = 0  # Default value if column doesn't exist in metrics
            
            # Add date and department
            new_row['Date'] = metrics['Date']
            new_row['Department'] = metrics['Department']
            
            # Check if row with same department and date already exists
            existing_row_mask = (master_df['Department'] == new_row['Department']) & (master_df['Date'] == new_row['Date'])
            
            if existing_row_mask.any():
                # Always overwrite existing row (regardless of null values)
                existing_row_idx = existing_row_mask.idxmax()
                for col in columns_to_edit:
                    if col in new_row:
                        master_df.at[existing_row_idx, col] = new_row[col]
                print(f"Overwrote existing row for {new_row['Department']} on {new_row['Date']}")
            else:
                # Append new row since it doesn't exist
                master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
                print(f"Added new row for {new_row['Department']} on {new_row['Date']}")
        else:
            # Check if row with same department and date already exists
            existing_row_mask = (master_df['Department'] == metrics['Department']) & (master_df['Date'] == metrics['Date'])
            
            if existing_row_mask.any():
                # Always overwrite existing row (regardless of null values)
                existing_row_idx = existing_row_mask.idxmax()
                for col in metrics.keys():
                    if col not in ['Date', 'Department']:  # Don't overwrite these
                        master_df.at[existing_row_idx, col] = metrics[col]
                print(f"Overwrote existing row for {metrics['Department']} on {metrics['Date']}")
            else:
                # Append new row since it doesn't exist
                master_df = pd.concat([master_df, pd.DataFrame([metrics])], ignore_index=True)
                print(f"Added new row for {metrics['Department']} on {metrics['Date']}")
        
        master_df.to_csv(master_csv_path, index=False)
        print(f"Successfully appended bot handle metrics to {master_csv_path}")
        
    except Exception as e:
        print(f"Error appending to master CSV: {e}")
        # Save metrics to separate CSV as fallback
        metrics_df = pd.DataFrame([metrics])
        metrics_df.to_csv(f"bot_handle_metrics_{department.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", index=False)
        print("Saved bot handle metrics to separate CSV as fallback")
    
    return metrics
