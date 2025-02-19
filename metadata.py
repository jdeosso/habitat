import json
import glob

# Path to your JSON lines file
vc_fm_path = '../../data/valor-cuota-fm/data/bpr-menu-*.jl'

# Use glob to handle wildcard in file path
file_list = glob.glob(vc_fm_path)

# Load a sample of the data
if file_list:
    with open(file_list[0], 'r') as file:
        # Read the first few lines
        sample_data = [json.loads(next(file)) for _ in range(5)]

    # Print the sample data
    for record in sample_data:
        print(record)
else:
    print("No files found matching the pattern.")