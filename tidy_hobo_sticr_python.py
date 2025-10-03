# import pandas as pd
# import os

# def tidy_hobo_sticr_python(faasr):
#     """
#     Simple Python version - processes STIC CSV files
#     """
    
#     print("Starting STIC data processing...")
    
#     # Step 1: Get list of CSV files from tutorial folder
#     folder_contents = faasr.faasr_get_folder_list(faasr_prefix="tutorial")
    
#     # Filter only CSV files
#     csv_files = [f for f in folder_contents if f.endswith('.csv')]
    
#     # Remove folder prefix
#     csv_files = [f.replace('tutorial/', '') for f in csv_files]
    
#     print(f"Found {len(csv_files)} CSV files")
    
#     if len(csv_files) == 0:
#         return "No CSV files found in tutorial folder"
    
#     # Step 2: Check which files need processing
#     files_to_process = []
    
#     for file_name in csv_files:
#         # Create output filename
#         output_name = file_name.replace('.csv', '_step1_tidy.csv')
        
#         # Check if already processed
#         try:
#             faasr.faasr_get_file(
#                 remote_folder="sticr-workflow/step1-tidy",
#                 remote_file=output_name,
#                 local_file="test_check.csv"
#             )
#             # File exists, delete test file
#             if os.path.exists("test_check.csv"):
#                 os.remove("test_check.csv")
#             print(f"SKIP: {file_name} (already processed)")
#         except:
#             # File doesn't exist, needs processing
#             files_to_process.append(file_name)
#             print(f"PROCESS: {file_name}")
    
#     if len(files_to_process) == 0:
#         return "All files already processed"
    
#     # Step 3: Process each file
#     processed_count = 0
    
#     for file_name in files_to_process:
#         try:
#             print(f"\nProcessing: {file_name}")
            
#             # Download file
#             faasr.faasr_get_file(
#                 remote_folder="tutorial",
#                 remote_file=file_name,
#                 local_file="input.csv"
#             )
            
#             # Read CSV
#             df = pd.read_csv("input.csv")
            
#             # Find columns (case-insensitive search)
#             columns = {col.lower(): col for col in df.columns}
            
#             datetime_col = None
#             temp_col = None
#             cond_col = None
            
#             # Find datetime column
#             for key, col in columns.items():
#                 if 'date' in key or 'time' in key:
#                     datetime_col = col
#                     break
            
#             # Find temperature column
#             for key, col in columns.items():
#                 if 'temp' in key:
#                     temp_col = col
#                     break
            
#             # Find conductivity column
#             for key, col in columns.items():
#                 if 'cond' in key or 'lux' in key or 'intensity' in key:
#                     cond_col = col
#                     break
            
#             if not datetime_col or not temp_col or not cond_col:
#                 print(f"ERROR: Could not find required columns in {file_name}")
#                 print(f"Available columns: {list(df.columns)}")
#                 continue
            
#             # Create standardized dataframe
#             tidy_df = pd.DataFrame({
#                 'datetime': pd.to_datetime(df[datetime_col]),
#                 'tempC': pd.to_numeric(df[temp_col], errors='coerce'),
#                 'condUncal': pd.to_numeric(df[cond_col], errors='coerce')
#             })
            
#             # Remove rows with missing data
#             tidy_df = tidy_df.dropna()
            
#             print(f"Processed {len(tidy_df)} rows")
            
#             # Save output
#             output_filename = file_name.replace('.csv', '_step1_tidy.csv')
#             tidy_df.to_csv("output.csv", index=False)
            
#             # Upload to MinIO
#             faasr.faasr_put_file(
#                 local_file="output.csv",
#                 remote_folder="sticr-workflow/step1-tidy",
#                 remote_file=output_filename
#             )
            
#             print(f"SUCCESS: Saved {output_filename}")
#             processed_count += 1
            
#         except Exception as e:
#             print(f"FAILED: {file_name} - {str(e)}")
    
#     return f"Completed: {processed_count} files processed"



import csv
import os
from datetime import datetime

def tidy_hobo_sticr_python(faasr):
    """
    Simple Python version - processes STIC CSV files
    Uses only built-in libraries (no pandas)
    """
    
    print("Starting STIC data processing...")
    
    # Step 1: Get list of CSV files from tutorial folder
    folder_contents = faasr.faasr_get_folder_list(faasr_prefix="tutorial")
    
    # Filter only CSV files
    csv_files = [f for f in folder_contents if f.endswith('.csv')]
    
    # Remove folder prefix
    csv_files = [f.replace('tutorial/', '') for f in csv_files]
    
    print(f"Found {len(csv_files)} CSV files")
    
    if len(csv_files) == 0:
        return "No CSV files found in tutorial folder"
    
    # Step 2: Check which files need processing
    files_to_process = []
    
    for file_name in csv_files:
        # Create output filename
        output_name = file_name.replace('.csv', '_step1_tidy.csv')
        
        # Check if already processed
        try:
            faasr.faasr_get_file(
                remote_folder="sticr-workflow/step1-tidy",
                remote_file=output_name,
                local_file="test_check.csv"
            )
            # File exists, delete test file
            if os.path.exists("test_check.csv"):
                os.remove("test_check.csv")
            print(f"SKIP: {file_name} (already processed)")
        except:
            # File doesn't exist, needs processing
            files_to_process.append(file_name)
            print(f"PROCESS: {file_name}")
    
    if len(files_to_process) == 0:
        return "All files already processed"
    
    # Step 3: Process each file
    processed_count = 0
    
    for file_name in files_to_process:
        try:
            print(f"\nProcessing: {file_name}")
            
            # Download file
            faasr.faasr_get_file(
                remote_folder="tutorial",
                remote_file=file_name,
                local_file="input.csv"
            )
            
            # Read CSV using built-in csv module
            with open("input.csv", 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if len(rows) == 0:
                print(f"ERROR: No data in {file_name}")
                continue
            
            # Find columns (case-insensitive search)
            columns = {col.lower(): col for col in rows[0].keys()}
            
            datetime_col = None
            temp_col = None
            cond_col = None
            
            # Find datetime column
            for key, col in columns.items():
                if 'date' in key or 'time' in key:
                    datetime_col = col
                    break
            
            # Find temperature column
            for key, col in columns.items():
                if 'temp' in key:
                    temp_col = col
                    break
            
            # Find conductivity column
            for key, col in columns.items():
                if 'cond' in key or 'lux' in key or 'intensity' in key:
                    cond_col = col
                    break
            
            if not datetime_col or not temp_col or not cond_col:
                print(f"ERROR: Could not find required columns in {file_name}")
                print(f"Available columns: {list(rows[0].keys())}")
                continue
            
            # Process and clean data
            tidy_rows = []
            for row in rows:
                try:
                    # Try to parse values
                    dt = row[datetime_col]
                    temp = float(row[temp_col])
                    cond = float(row[cond_col])
                    
                    # Add to output if all values are valid
                    tidy_rows.append({
                        'datetime': dt,
                        'tempC': temp,
                        'condUncal': cond
                    })
                except (ValueError, KeyError):
                    # Skip rows with invalid data
                    continue
            
            if len(tidy_rows) == 0:
                print(f"ERROR: No valid data after processing {file_name}")
                continue
            
            print(f"Processed {len(tidy_rows)} rows")
            
            # Save output using csv module
            output_filename = file_name.replace('.csv', '_step1_tidy.csv')
            with open("output.csv", 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['datetime', 'tempC', 'condUncal'])
                writer.writeheader()
                writer.writerows(tidy_rows)
            
            # Upload to MinIO
            faasr.faasr_put_file(
                local_file="output.csv",
                remote_folder="sticr-workflow/step1-tidy",
                remote_file=output_filename
            )
            
            print(f"SUCCESS: Saved {output_filename}")
            processed_count += 1
            
        except Exception as e:
            print(f"FAILED: {file_name} - {str(e)}")
    
    return f"Completed: {processed_count} files processed"