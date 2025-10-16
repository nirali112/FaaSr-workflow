import csv
import os

def tidy_hobo_sticr_python():
    
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "tutorialSTICR")
    
    csv_files = []
    for f in folder_contents:
        if f.startswith('tutorialSTICR/') and f.endswith('.csv'):
            if f.count('/') == 1:
                csv_files.append(f)
    
    csv_files = [f.replace('tutorialSTICR/', '') for f in csv_files]
    
    files_to_process = []
    
    for file_name in csv_files:
        output_name = file_name.replace('.csv', '_step1_tidy.csv')
        files_to_process.append(file_name)
    
    test_file = files_to_process[0]
    
    faasr_get_file(
        server_name="My_S3_Bucket",
        remote_folder="tutorialSTICR",
        remote_file=test_file,
        local_folder="",
        local_file="input.csv"
    )
    
    print("File downloaded")
    
    with open("input.csv", 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"Read {len(rows)} rows")
    print(f"Columns: {list(rows[0].keys())}")