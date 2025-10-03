import pandas as pd
import os
from datetime import datetime

def tidy_hobo_sticr_python(faasr):
    """
    Python version of the STIC data processing function
    """
    print("Starting STIC data processing...")
    
    # Step 1: Get list of CSV files
    folder_contents = faasr.faasr_get_folder_list(faasr_prefix="tutorial")
    
    # Convert list to character vector and filter for CSV files
    csv_files = [f for f in folder_contents if f.endswith('.csv')]
    
    # Remove folder prefix
    csv_files = [f.replace('tutorial/', '') for f in csv_files]
    print(f"Found {len(csv_files)} CSV files")
    
    if len(csv_files) == 0:
        print("No files found in bucket!")
        print("Make sure files are uploaded to 'tutorial' folder")
        return "No files found to process"
    
    # Step 2: Check which files need processing
    files_to_process = []
    
    for file_name in csv_files:
        # Create output filename
        clean_filename = file_name.replace('.csv', '')
        output_name = f"{clean_filename}_step1_tidy.csv"
        
        # Check if already processed
        try:
            faasr.faasr_get_file(
                remote_folder="sticr-workflow/step1-tidy",
                remote_file=output_name,
                local_file=f"test_step1_{output_name}"
            )
            # File exists, delete test file
            test_file = f"test_step1_{output_name}"
            if os.path.exists(test_file):
                os.remove(test_file)
            print(f"Already processed - SKIPPING: {output_name}")
        except:
            # File doesn't exist, needs processing
            files_to_process.append(file_name)
            print(f"Not yet processed - WILL PROCESS: {file_name}")
    
    if len(files_to_process) == 0:
        print("All files already processed! No new files to tidy.")
        return "All files already processed - no new tidying needed"
    
    print(f"Found {len(csv_files)} raw files, {len(files_to_process)} need processing")
    for file in files_to_process:
        print(f"- {file}")
    
    # Step 3: Process each file
    processed_files = 0
    processing_results = {}
    
    for file_name in files_to_process:
        try:
            # Download file
            faasr.faasr_get_file(
                remote_folder="tutorial",
                remote_file=file_name,
                local_file="current_input.csv"
            )
            print(f"Downloaded: {file_name}")
            
            # Read first few lines for detection
            with open("current_input.csv", 'r') as f:
                first_lines = [f.readline().strip() for _ in range(10)]
            
            # Enhanced detection logic matching R version
            is_raw_hobo = any(any(marker in line.lower() for marker in 
                ["#", "plot title", "lgr s/n", "temp", "°c", "lgr", "series:", "hoboware"]) 
                for line in first_lines[:5])
            
            has_project_column = any("project" in line.lower() and "datetime" in line.lower() 
                                   and "siteid" in line.lower() for line in first_lines[:3])
            
            # Read and process data
            if is_raw_hobo:
                print("DETECTED: RAW HOBO DATA")
                df = pd.read_csv("current_input.csv", comment="#")
                processing_method = "hobo_data_processing"
            elif has_project_column:
                print("DETECTED: PROCESSED RESEARCH DATA - Using custom tidying")
                df = pd.read_csv("current_input.csv")
                processing_method = "research_data_processing"
            else:
                print("DETECTED: UNKNOWN FORMAT - Attempting generic processing")
                df = pd.read_csv("current_input.csv")
                processing_method = "generic_processing"
            
            # Find columns (case-insensitive search)
            columns = {col.lower(): col for col in df.columns}
            
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
                raise ValueError(f"Could not find required columns. Found: {list(df.columns)}")
            
            # Create standardized dataframe
            tidy_df = pd.DataFrame({
                'datetime': pd.to_datetime(df[datetime_col]),
                'tempC': pd.to_numeric(df[temp_col], errors='coerce'),
                'condUncal': pd.to_numeric(df[cond_col], errors='coerce')
            })
            
            # Remove rows with missing data
            tidy_df = tidy_df.dropna()
            
            if len(tidy_df) == 0:
                raise ValueError("No valid data after processing")
            
            print(f"Processed {len(tidy_df)} rows")
            
            # Generate output filename
            clean_filename = file_name.replace('.csv', '')
            output_filename = f"{clean_filename}_step1_tidy.csv"
            
            # Save locally
            tidy_df.to_csv(output_filename, index=False)
            
            # Upload to MinIO
            faasr.faasr_put_file(
                local_file=output_filename,
                remote_folder="sticr-workflow/step1-tidy",
                remote_file=output_filename
            )
            
            # Store processing results
            processing_results[file_name] = {
                'status': 'SUCCESS',
                'method': processing_method,
                'input_file': file_name,
                'output_file': output_filename,
                'rows_processed': len(tidy_df),
                'date_range_start': str(tidy_df['datetime'].min()),
                'date_range_end': str(tidy_df['datetime'].max()),
                'processing_time': str(datetime.now())
            }
            
            print(f"SUCCESS: Saved {output_filename}")
            processed_files += 1
            
        except Exception as e:
            print(f"FAILED to process {file_name}: {str(e)}")
            processing_results[file_name] = {
                'status': 'FAILED',
                'error_message': str(e),
                'processing_time': str(datetime.now())
            }
    
    return f"Step 1 tidying completed: {processed_files} new files processed, {len(csv_files) - len(files_to_process)} files skipped (already processed)"