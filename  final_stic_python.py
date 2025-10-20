import csv
import os

def final_sticr_python():
    
    print("Step 4 Final - Starting")
    
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "sticr-workflow/step3-classified")
    
    step3_files = []
    for f in folder_contents:
        if f.endswith('.csv'):
            step3_files.append(f)
    
    step3_files = [f.replace('sticr-workflow/step3-classified/', '') for f in step3_files]
    
    files_to_process = []
    
    for file_name in step3_files:
        clean_filename = file_name.replace('_step3_classified.csv', '')
        output_name = clean_filename + '_step4_final.csv'
        
        try:
            faasr_get_file(
                server_name="My_S3_Bucket",
                remote_folder="sticr-workflow/step4-final",
                remote_file=output_name,
                local_folder="",
                local_file="test_check.csv"
            )
            if os.path.exists("test_check.csv"):
                os.remove("test_check.csv")
        except:
            files_to_process.append(file_name)
    
    if len(files_to_process) == 0:
        print("All files already finalized")
        return
    
    print(f"Processing {len(files_to_process)} files")
    
    for file_name in files_to_process:
        
        faasr_get_file(
            server_name="My_S3_Bucket",
            remote_folder="sticr-workflow/step3-classified",
            remote_file=file_name,
            local_folder="",
            local_file="input.csv"
        )
        
        with open("input.csv", 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        final_rows = []
        wet_count = 0
        dry_count = 0
        spc_values = []
        
        for row in rows:
            try:
                dt = row['datetime']
                cond_uncal = float(row['condUncal'])
                temp = float(row['tempC'])
                spc = float(row['SpC'])
                wetdry = row['wetdry']
                
                qaqc = ""
                if spc < 0:
                    qaqc = "negative_spc"
                
                if wetdry == 'wet':
                    wet_count += 1
                else:
                    dry_count += 1
                
                spc_values.append(spc)
                
                final_rows.append({
                    'datetime': dt,
                    'condUncal': cond_uncal,
                    'tempC': temp,
                    'SpC': spc,
                    'wetdry': wetdry,
                    'QAQC': qaqc
                })
            except:
                continue
        
        wet_percentage = round((wet_count / len(final_rows)) * 100, 1) if len(final_rows) > 0 else 0
        min_spc = round(min(spc_values), 1) if spc_values else 0
        max_spc = round(max(spc_values), 1) if spc_values else 0
        
        print(f"Final dataset: {len(final_rows)} rows")
        print(f"Wet: {wet_count} ({wet_percentage}%) | Dry: {dry_count}")
        print(f"SpC range: {min_spc} - {max_spc} µS/cm")
        
        clean_filename = file_name.replace('_step3_classified.csv', '')
        output_filename = clean_filename + '_step4_final.csv'
        
        with open("output.csv", 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['datetime', 'condUncal', 'tempC', 'SpC', 'wetdry', 'QAQC'])
            writer.writeheader()
            writer.writerows(final_rows)
        
        faasr_put_file(
            server_name="My_S3_Bucket",
            local_folder="",
            local_file="output.csv",
            remote_folder="sticr-workflow/step4-final",
            remote_file=output_filename
        )
        
        print(f"Uploaded: {output_filename}")
    
    print("STICr Workflow Complete!")