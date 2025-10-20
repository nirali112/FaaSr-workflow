import csv
import os

def classify_wetdry_python():
    
    print("Step 3 Classify - Starting")
    
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "sticr-workflow/step2-calibrated")
    
    step2_files = []
    for f in folder_contents:
        if f.endswith('.csv'):
            step2_files.append(f)
    
    step2_files = [f.replace('sticr-workflow/step2-calibrated/', '') for f in step2_files]
    
    files_to_process = []
    
    for file_name in step2_files:
        clean_filename = file_name.replace('_step2_calibrated.csv', '')
        output_name = clean_filename + '_step3_classified.csv'
        
        try:
            faasr_get_file(
                server_name="My_S3_Bucket",
                remote_folder="sticr-workflow/step3-classified",
                remote_file=output_name,
                local_folder="",
                local_file="test_check.csv"
            )
            if os.path.exists("test_check.csv"):
                os.remove("test_check.csv")
        except:
            files_to_process.append(file_name)
    
    if len(files_to_process) == 0:
        print("All files already classified")
        return
    
    print(f"Processing {len(files_to_process)} files")
    
    for file_name in files_to_process:
        
        faasr_get_file(
            server_name="My_S3_Bucket",
            remote_folder="sticr-workflow/step2-calibrated",
            remote_file=file_name,
            local_folder="",
            local_file="input.csv"
        )
        
        with open("input.csv", 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        classified_rows = []
        wet_count = 0
        dry_count = 0
        
        threshold = 100
        
        for row in rows:
            try:
                dt = row['datetime']
                cond_uncal = float(row['condUncal'])
                temp = float(row['tempC'])
                spc = float(row['SpC'])
                
                if spc >= threshold:
                    wetdry = 'wet'
                    wet_count += 1
                else:
                    wetdry = 'dry'
                    dry_count += 1
                
                classified_rows.append({
                    'datetime': dt,
                    'condUncal': cond_uncal,
                    'tempC': temp,
                    'SpC': spc,
                    'wetdry': wetdry
                })
            except:
                continue
        
        print(f"Classified {len(classified_rows)} rows - Wet: {wet_count}, Dry: {dry_count}")
        
        clean_filename = file_name.replace('_step2_calibrated.csv', '')
        output_filename = clean_filename + '_step3_classified.csv'
        
        with open("output.csv", 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['datetime', 'condUncal', 'tempC', 'SpC', 'wetdry'])
            writer.writeheader()
            writer.writerows(classified_rows)
        
        faasr_put_file(
            server_name="My_S3_Bucket",
            local_folder="",
            local_file="output.csv",
            remote_folder="sticr-workflow/step3-classified",
            remote_file=output_filename
        )
        
        print(f"Uploaded: {output_filename}")