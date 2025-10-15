def tidy_hobo_sticr_python():
    """
    Step 1 TEST: Just get folder list
    """
    
    
    # Get list of files from tutorial folder
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "tutorial")
    
    print(f"Raw folder contents: {folder_contents}")
    print(f"Type: {type(folder_contents)}")
    
    # Filter only CSV files
    csv_files = [f for f in folder_contents if f.endswith('.csv')]
    
    print(f"Found {len(csv_files)} CSV files:")
    for f in csv_files:
        print(f"  - {f}")
    
    return f"Step 1 test complete: Found {len(csv_files)} CSV files"
