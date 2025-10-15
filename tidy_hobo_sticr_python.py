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
        
        # try:
        #     faasr_get_file("My_S3_Bucket", "sticr-workflow/step1-tidy", output_name, "", "test_check.csv")
        #     if os.path.exists("test_check.csv"):
        #         os.remove("test_check.csv")
        # except:
        files_to_process.append(file_name)

    #step 3
    # Step 3: Process and tidy each file
    for file_name in files_to_process:
    # Ensure file_name has no prefix
        clean_name = file_name.replace('tutorialSTICR/', '')

        # Download file from S3
        s3_key = f"tutorialSTICR/{clean_name}"
        local_input = "input.csv"

        try:
            faasr_get_file("My_S3_Bucket", "tutorialSTICR", clean_name, "", local_input)
        except Exception as e:
            print(f" Failed to download {s3_key}: {e}")
            continue

        # Read CSV and clean it
        with open(local_input, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            print(f" Skipping {clean_name}: empty CSV")
            continue

        columns = {col.lower(): col for col in rows[0].keys()}

        datetime_col = next((col for key, col in columns.items() if 'date' in key or 'time' in key), None)
        temp_col = next((col for key, col in columns.items() if 'temp' in key), None)
        cond_col = next((col for key, col in columns.items() if any(k in key for k in ['cond', 'lux', 'intensity'])), None)

        tidy_rows = []
        for row in rows:
            try:
                tidy_rows.append({
                    'datetime': row[datetime_col],
                    'tempC': float(row[temp_col]),
                    'condUncal': float(row[cond_col])
                })
            except (ValueError, KeyError, TypeError):
                continue

        output_filename = clean_name.replace('.csv', '_step1_tidy.csv')

        with open("output.csv", 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['datetime', 'tempC', 'condUncal'])
            writer.writeheader()
            writer.writerows(tidy_rows)

        try:
            faasr_put_file("My_S3_Bucket", "", "output.csv", "sticr-workflow/step1-tidy", output_filename)
            print(f" Processed: {clean_name}")
        except Exception as e:
            print(f" Failed to upload {output_filename}: {e}")
