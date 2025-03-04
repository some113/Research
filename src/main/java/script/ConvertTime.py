import os
import glob
from datetime import datetime

# Function to convert timestamp to Unix time
def convert_to_unix(timestamp):
    try:
        # Use %d/%m/%Y %H:%M:%S in 24-hour format
        dt = datetime.strptime(timestamp.strip(), "%d/%m/%Y %I:%M:%S %p")
        return str(int(dt.timestamp()))  # Convert to string for writing back
    except ValueError:
        #print(f"Failed to convert: {timestamp}")
        return timestamp.strip()  # Return original if conversion fails

# Folder containing input CSV files
input_folder = os.path.dirname(os.path.realpath(__file__)) + "\\..\\..\\..\\..\\InputData\\Flow\\"  # Change this to your actual folder

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
            parts[3] = convert_to_unix(parts[3])  # Modify second column
        modified_lines.append(",".join(parts))  # Reconstruct line

    # Overwrite the original file with modified content
    with open(file_path, "w", encoding="utf-8") as outfile:
        outfile.write("\n".join(modified_lines) + "\n")  # Write back

    print(f"Modified: {file_path}")

print("Batch modification completed!")
