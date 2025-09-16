import pandas as pd

def my_test_function(faasr):
    """Simple test function"""
    
    # Get arguments from JSON config
    folder = faasr["InvokeServerArguments"]["folder"]
    output = faasr["InvokeServerArguments"]["output"]
    
    # Create some test data
    data = {
        'test_numbers': [10, 20, 30, 40, 50],
        'doubled': [20, 40, 60, 80, 100]
    }
    
    df = pd.DataFrame(data)
    
    # Save the output
    df.to_csv(output, index=False)
    
    print(f"Test function completed! Created {output}")
