# import csv
# import os

# def calibrate_sticr_python():
    
#     folder_contents = faasr_get_folder_list("My_S3_Bucket", "sticr-workflow/step1-tidy")
    
#     step1_files = []
#     for f in folder_contents:
#         if f.endswith('.csv'):
#             step1_files.append(f)
    
#     step1_files = [f.replace('sticr-workflow/step1-tidy/', '') for f in step1_files]
    
#     print(f"Found {len(step1_files)} Step 1 files")
    
#     files_to_process = []
    
#     for file_name in step1_files:
#         clean_filename = file_name.replace('_step1_tidy.csv', '')
#         output_name = clean_filename + '_step2_calibrated.csv'
        
#         try:
#             faasr_get_file(
#                 server_name="My_S3_Bucket",
#                 remote_folder="sticr-workflow/step2-calibrated",
#                 remote_file=output_name,
#                 local_folder="",
#                 local_file="test_check.csv"
#             )
#             if os.path.exists("test_check.csv"):
#                 os.remove("test_check.csv")
#             print(f"SKIP: {file_name}")
#         except:
#             files_to_process.append(file_name)
    
#     if len(files_to_process) == 0:
#         print("All files already processed")
#         return
    
#     for file_name in files_to_process:
        
#         faasr_get_file(
#             server_name="My_S3_Bucket",
#             remote_folder="sticr-workflow/step1-tidy",
#             remote_file=file_name,
#             local_folder="",
#             local_file="input.csv"
#         )
        
#         with open("input.csv", 'r') as f:
#             reader = csv.DictReader(f)
#             rows = list(reader)
        
#         calibrated_rows = []
#         for row in rows:
#             try:
#                 dt = row['datetime']
#                 temp = float(row['tempC'])
#                 cond_uncal = float(row['condUncal'])
                
#                 spc = cond_uncal * 1.5
                
#                 calibrated_rows.append({
#                     'datetime': dt,
#                     'condUncal': cond_uncal,
#                     'tempC': temp,
#                     'SpC': spc
#                 })
#             except:
#                 continue
        
#         clean_filename = file_name.replace('_step1_tidy.csv', '')
#         output_filename = clean_filename + '_step2_calibrated.csv'
        
#         with open("output.csv", 'w', newline='') as f:
#             writer = csv.DictWriter(f, fieldnames=['datetime', 'condUncal', 'tempC', 'SpC'])
#             writer.writeheader()
#             writer.writerows(calibrated_rows)
        
#         faasr_put_file(
#             server_name="My_S3_Bucket",
#             local_folder="",
#             local_file="output.csv",
#             remote_folder="sticr-workflow/step2-calibrated",
#             remote_file=output_filename
#         )
        
#         print(f"Processed: {file_name}")


import csv
import os

def calibrate_sticr_python():
    
    print("Step 2 Calibrate - Starting")
    
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "sticr-workflow/step1-tidy")
    
    print(f"Raw folder contents: {folder_contents}")
    
    step1_files = []
    for f in folder_contents:
        if f.endswith('.csv'):
            step1_files.append(f)
    
    print(f"Found {len(step1_files)} CSV files")