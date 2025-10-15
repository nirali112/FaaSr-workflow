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
        
        try:
            faasr_get_file("My_S3_Bucket", "sticr-workflow/step1-tidy", output_name, "", "test_check.csv")
            if os.path.exists("test_check.csv"):
                os.remove("test_check.csv")
        except:
            files_to_process.append(file_name)
    
    for file_name in files_to_process:
        
        faasr_get_file("My_S3_Bucket", "tutorialSTICR", file_name, "", "input.csv")
        
        with open("input.csv", 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        columns = {col.lower(): col for col in rows[0].keys()}
        
        datetime_col = None
        temp_col = None
        cond_col = None
        
        for key, col in columns.items():
            if 'date' in key or 'time' in key:
                datetime_col = col
                break
        
        for key, col in columns.items():
            if 'temp' in key:
                temp_col = col
                break
        
        for key, col in columns.items():
            if 'cond' in key or 'lux' in key or 'intensity' in key:
                cond_col = col
                break
        
        tidy_rows = []
        for row in rows:
            try:
                dt = row[datetime_col]
                temp = float(row[temp_col])
                cond = float(row[cond_col])
                
                tidy_rows.append({
                    'datetime': dt,
                    'tempC': temp,
                    'condUncal': cond
                })
            except:
                continue
        
        output_filename = file_name.replace('.csv', '_step1_tidy.csv')
        with open("output.csv", 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['datetime', 'tempC', 'condUncal'])
            writer.writeheader()
            writer.writerows(tidy_rows)
        
        faasr_put_file("My_S3_Bucket", "", "output.csv", "sticr-workflow/step1-tidy", output_filename)
        
        print(f"Processed: {file_name}")