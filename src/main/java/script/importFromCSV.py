import pandas as pd

def extract_columns(input_file, output_file, columns):
    """
    Extract specific columns from a CSV file and save to a new CSV file.
    
    :param input_file: Path to the input CSV file.
    :param output_file: Path to save the extracted columns.
    :param columns: List of column names to extract.
    """
    try:
        # Load CSV file
        df = pd.read_csv(input_file)
        
        # Extract specified columns
        extracted_df = df[columns]
        
        # Save to a new CSV file
        extracted_df.to_csv(output_file, index=False)
        
        print(f"Extracted columns saved to {output_file}")
    except Exception as e:
        print(f"Error: {e}")

# Example usage
input_csv = "E:/VM/Shared/ISOT_Botnet_DataSet_2010.pcap_Flow_Extracted.csv"
output_csv = "E:/VM/Shared/ISOT_extracted.csv"
columns_to_extract = ["Column1", "Column2"]  # Replace with actual column names

extract_columns(input_csv, output_csv, columns_to_extract)