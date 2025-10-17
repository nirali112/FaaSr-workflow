import csv
import os

def calibrate_sticr_python():
    
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "sticr-workflow/step1-tidy")
    
    step1_files = []
    for f in folder_contents:
        if f.endswith('.csv'):
            step1_files.append(f)
    
    step1_files = [f.replace('sticr-workflow/step1-tidy/', '') for f in step1_files]
    
    print(f"Found {len(step1_files)} Step 1 files:")
    for f in step1_files:
        print(f"  - {f}")