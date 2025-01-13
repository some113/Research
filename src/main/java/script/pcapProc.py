import os
import re

# Dictionary to store processed data (acts like a hash map)
startTimeDict = {}
flowDict = {}
sumIn = {}
sumOut = {}
cntIn = {}
cntOut = {}

TIME_GAP = 30

# Input and output file paths
input_file_path = "C:\\Code\\out.txt"  # Replace with your input file
output_file_path = "C:\\Code\\processedOutput.txt"  # Replace with your output file

inRange = r"^147\.32\.\d{1,3}\.\d{1,3}$"

# Ensure the output file is empty before writing
if os.path.exists(output_file_path):
    os.remove(output_file_path)

# Open the input file
with open(input_file_path, 'r') as infile:
    # Read and process each line
    for line in infile:
        
        line = line.strip()
        data = line.split("\t")
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
        size = int(data[4])

        ipPattern = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.\d+$"
        if re.match(ipPattern, srcAdd) is None or re.match(ipPattern, dstAdd) is None:
            continue
        key = "" + proto + " " + srcAdd + " " + dstAdd + " "

        if size == "0":
            continue
        # Example condition: Only store the line if the key is not already in the dictionary
        if key in startTimeDict:
            if time - startTimeDict[key] > TIME_GAP:
                # Optionally write this processed line to the output file
                with open(output_file_path, 'a') as outfile:
                    outfile.write(f"{srcAdd} {proto},{dstAdd},")
                    #outfile.write(",".join(flowDict[key]) + "\n")
                    #avg = int(sumDict[key] / cntDict[key])
                    
                    outfile.write(f"{int(sumIn[key] / cntIn[key]) if cntIn[key] != 0 else 0},{int(sumOut[key] / cntOut[key]) if cntOut[key] != 0 else 0}\n")
                    startTimeDict[key] = time
                    if re.match(srcAdd, inRange):
                        sumOut[key] = size
                        cntOut[key] = 1
                    else:
                        sumIn[key] = size
                        cntIn[key] = 1
                    flowDict[key] = [size]
            else:
                flowDict[key].append(size)
                if re.match(srcAdd, inRange):
                    sumOut[key] += size
                    cntOut[key] += 1
                else:
                    sumIn[key] += size
                    cntIn[key] += 1

        else:
            startTimeDict[key] = time
            flowDict[key] = [size] 
            sumOut[key] = 0
            cntOut[key] = 0
            sumIn[key] = 0
            cntIn[key] = 0
            if re.match(srcAdd, inRange):
                sumOut[key] = size
                cntOut[key] = 1
            else:
                sumIn[key] = size
                cntIn[key] = 1
for key in startTimeDict:
    with open(output_file_path, 'a') as outfile:
        outfile.write(f"{srcAdd} {proto},{dstAdd},")
        #outfile.write(",".join(flowDict[key]) + "\n")
        
        #avg = int(f"{int(sumIn[key] / cntIn[key]) if cntIn[key] != 0 else 0},{int(sumOut[key] / cntOut[key]) if cntOut[key] != 0 else 0}\n")
        outfile.write(f"{int(sumIn[key] / cntIn[key]) if cntIn[key] != 0 else 0},{int(sumOut[key] / cntOut[key]) if cntOut[key] != 0 else 0}\n")
    outfile.close()


# Now `data_dict` contains the processed data across all lines
print("Processed data (dictionary):")
