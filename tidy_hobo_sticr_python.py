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
    if not files_to_process:
        print("DEBUG: No files found to process (files_to_process is empty).")
        return
    
    # pick first file to test the get_file call (keeps scope minimal)
    test_file = files_to_process[0]
    
    # make sure it's clean
    test_file = test_file.replace('tutorialSTICR/', '')
    
    print("DEBUG: about to call faasr_get_file(...)")
    print("DEBUG: server_name =", repr("My_S3_Bucket"))
    print("DEBUG: remote_folder =", repr("tutorialSTICR"))
    print("DEBUG: remote_file =", repr(test_file))
    print("DEBUG: local_folder =", repr(""))
    print("DEBUG: local_file =", repr("input.csv"))
    # flush to ensure immediate console output in some runners
    try:
        import sys
        sys.stdout.flush()
    except:
        pass

    try:
        # Use named args to avoid positional confusion
        faasr_get_file(
            server_name="My_S3_Bucket",
            remote_folder="tutorialSTICR",
            remote_file=test_file,
            local_folder="",
            local_file="input.csv"
        )
        print("DEBUG: faasr_get_file() returned without raising an exception.")
    except Exception as e:
        print("DEBUG: Exception raised by faasr_get_file():", repr(e))
        print("DEBUG: Full traceback:")
        traceback.print_exc()
    
    print("DEBUG: End of minimal test. Returning from function.")
    return