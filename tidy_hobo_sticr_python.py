def tidy_hobo_sticr_python():
    """
    Step 1: Get folder list and filter
    """
    
    print("Starting STIC data processing...")
    
    # Get list of files from tutorial folder
    folder_contents = faasr_get_folder_list("My_S3_Bucket", "tutorial")
    
    # Filter only CSV files that are directly in tutorial/ (not subfolders)
    csv_files = []
    for f in folder_contents:
        # Only files that start with "tutorial/" and have no more slashes after
        if f.startswith('tutorial/') and f.endswith('.csv'):
            # Count slashes - should be exactly 1 (tutorial/filename.csv)
            if f.count('/') == 1:
                csv_files.append(f)
    
    # Remove the "tutorial/" prefix
    csv_files = [f.replace('tutorial/', '') for f in csv_files]
    
    print(f"Found {len(csv_files)} CSV files in tutorial folder:")
    for f in csv_files:
        print(f"  - {f}")
    
    # Return None or empty dict instead of string
    return None
