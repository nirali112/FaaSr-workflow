import csv

def my_test_function(faasr):
    
    # Get arguments from JSON config
    folder = faasr["InvokeServerArguments"]["folder"]
    output = faasr["InvokeServerArguments"]["output"]
    
    # Create test data
    data = [
        ['test_numbers', 'doubled'],  # header
        [10, 20],
        [20, 40],
        [30, 60],
        [40, 80],
        [50, 100]
    ]
    
    # Write CSV file using built-in csv module
    with open(output, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)
    
    print(f"Test function completed! Created {output}")
