import os
import glob
from datetime import datetime

# Function to convert timestamp to Unix time
def convert_to_unix(timestamp):
    try:
        dt = datetime.strptime(timestamp.strip(), "%Y-%m-%d %H:%M:%S")
        return str(int(dt.timestamp()))  # Convert to string for writing back
    except ValueError:
        return timestamp.strip()  # Return original if conversion fails

# Folder containing input CSV files
input_folder = "C:\\Newfolder\\Research\\InputData\\Flow\\"  # Change this to your actual folder

# Get all CSV files in input folder
input_files = glob.glob(os.path.join(input_folder, "*.csv"))

for file_path in input_files:
    # Read the file and modify in memory
    with open(file_path, "r", encoding="utf-8") as infile:
        lines = infile.readlines()

    # Process each line
    modified_lines = []
    for line in lines:
        parts = line.strip().split(",")  # Split by ","
        if len(parts) > 1:  # Ensure there's a second column
            parts[1] = convert_to_unix(parts[1])  # Modify second column
        modified_lines.append(",".join(parts))  # Reconstruct line

    # Overwrite the original file with modified content
    with open(file_path, "w", encoding="utf-8") as outfile:
        outfile.write("\n".join(modified_lines) + "\n")  # Write back

    print(f"Modified: {file_path}")

print("Batch modification completed!")
