import csv
import os

def calibrate_stic_python():
    
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "sticr-workflow/step1-tidy")
    
    step1_files = []
    for f in folder_contents:
        if f.endswith('.csv'):
            step1_files.append(f)
    
    step1_files = [f.replace('sticr-workflow/step1-tidy/', '') for f in step1_files]
    
    print(f"Found {len(step1_files)} Step 1 files")
    
    files_to_process = []
    
    for file_name in step1_files:
        clean_filename = file_name.replace('_step1_tidy.csv', '')
        output_name = clean_filename + '_step2_calibrated.csv'
        
        try:
            faasr_get_file(
                server_name="My_S3_Bucket",
                remote_folder="sticr-workflow/step2-calibrated",
                remote_file=output_name,
                local_folder="",
                local_file="test_check.csv"
            )
            if os.path.exists("test_check.csv"):
                os.remove("test_check.csv")
            print(f"SKIP: {file_name}")
        except:
            files_to_process.append(file_name)
    
    print(f"Files to process: {len(files_to_process)}")