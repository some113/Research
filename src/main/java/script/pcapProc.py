import os
import re

def process_file(input_file_path, output_file_path):
    startTimeDict = {}
    lastTimeDict = {}
    flowDict = {}
    TIME_GAP = 30

    inRange = r"^147\.32\.\d{1,3}\.\d{1,3}$"
    
    try:
        with open(input_file_path, 'r') as infile:
            for line in infile:
                line = line.strip()
                data = line.split(" ")
                
                if len(data) < 5:
                    continue
                
                try:
                    time = float(data[0])
                    #print(time)
                except ValueError:
                    print("Error: Invalid time format in line:", line)
                    continue
                
                proto = data[1]
                srcAdd = data[2]
                dstAdd = data[3]
                size = data[4]
                #print(f"{srcAdd} {dstAdd}")
                # IP address + port
                ipPattern = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\.\d+$"
                if re.match(ipPattern, srcAdd) is None or re.match(ipPattern, dstAdd) is None:
                    continue
                
                key = f"{proto} {srcAdd} {dstAdd}"
                
                if size == "0":
                    continue
                
                if key in startTimeDict:
                    if time - lastTimeDict[key] > TIME_GAP:
                        with open(output_file_path, 'a') as outfile:
                            outfile.write(f"{startTimeDict[key]} {key} {','.join(flowDict[key])}\n")
                            outfile.flush()
                            # print(f"{time} {key} {','.join(flowDict[key])}")
                        startTimeDict[key] = time
                        lastTimeDict[key] = time
                        flowDict[key] = [size]
                    else:
                        flowDict[key].append(size)
                        lastTimeDict[key] = time
                else:
                    startTimeDict[key] = time
                    lastTimeDict[key] = time
                    flowDict[key] = [size]
        
        for key in startTimeDict:
            with open(output_file_path, 'a') as outfile:
                outfile.write(f"{startTimeDict[key]} {key} {','.join(flowDict[key])}\n")
                outfile.close() 
        
        print(f"Successfully processed: {input_file_path}")
        print(f"Output saved to: {output_file_path}")
    except Exception as e:
        print(f"Skipping file {input_file_path} due to error: {e}")

# Directory containing input files
root_dir = os.path.dirname(os.path.abspath(__file__)) + "\\..\\..\\..\\..\\"
input_folder = root_dir + "tempFolderForPcapProcess"
output_folder = "C:\\Newfolder\\Research\\InputData\\Flow" #root_dir + "InputData\\Flow"

# Process all files in the input folder
for filename in os.listdir(input_folder):
    input_file_path = os.path.join(input_folder, filename)
    output_file_path = os.path.join(output_folder, f"processed_{filename}")
    
    if os.path.isfile(input_file_path):
        process_file(input_file_path, output_file_path)

"""
import os
import re

# Dictionary to store processed data (acts like a hash map)
startTimeDict = {}
flowDict = {}
sum = {}
cnt = {}

TIME_GAP = 30

# Input and output file paths
input_file_path = "C:\\Code\\Research\\InputData\\Flow\\ISOT_out.txt"  # Replace with your input file
output_file_path = "C:\\Code\\Research\\InputData\\Flow\\ISOT_processedOutput.txt"  # Replace with your output file

inRange = r"^147\.32\.\d{1,3}\.\d{1,3}$"

# Ensure the output file is empty before writing
if os.path.exists(output_file_path):
    os.remove(output_file_path)

# Open the input file
with open(input_file_path, 'r') as infile:
    # Read and process each line
    for line in infile:
        #print(line)
        line = line.strip()
        data = line.split(" ")
        #print(data)

        if len(data) < 5:
            continue
        try:
            time = float(data[0])
        except ValueError:
            continue
        proto = data[1]
        srcAdd = data[2]
        dstAdd = data[3]
        size = data[4]

        ipPattern = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.\d+$"
        if re.match(ipPattern, srcAdd) is None or re.match(ipPattern, dstAdd) is None:
            continue
        key = "" + proto + " " + srcAdd + " " + dstAdd + " "
        #print(key)

        if size == "0":
            continue
        # Example condition: Only store the line if the key is not already in the dictionary
        if key in startTimeDict:
            if time - startTimeDict[key] > TIME_GAP:
                # Optionally write this processed line to the output file
                with open(output_file_path, 'a') as outfile:
                    outfile.write(f"{time} {key} ")
                    outfile.write(",".join(flowDict[key]) + "\n")
                    #avg = int(sumDict[key] / cntDict[key])
                    
                    #outfile.write(f"{int(sumIn[key] / cntIn[key]) if cntIn[key] != 0 else 0},{int(sumOut[key] / cntOut[key]) if cntOut[key] != 0 else 0}\n")
                    startTimeDict[key] = time
                    flowDict[key] = [size]
            else:
                flowDict[key].append(size)

        else:
            startTimeDict[key] = time
            flowDict[key] = [size] 
for key in startTimeDict:
    with open(output_file_path, 'a') as outfile:
        outfile.write(f"{time} {key} ")
        outfile.write(",".join(flowDict[key]) + "\n")
        
        #avg = int(f"{int(sumIn[key] / cntIn[key]) if cntIn[key] != 0 else 0},{int(sumOut[key] / cntOut[key]) if cntOut[key] != 0 else 0}\n")
        #outfile.write(f"{int(sumIn[key] / cntIn[key]) if cntIn[key] != 0 else 0},{int(sumOut[key] / cntOut[key]) if cntOut[key] != 0 else 0}\n")
    outfile.close()


# Now `data_dict` contains the processed data across all lines
print("Processed data (dictionary):")
*/
"""