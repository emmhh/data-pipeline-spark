import csv
import json

# Function to infer data type based on column name and position
def infer_data_type(column_name, index, total_columns):
    # Define specific conditions for known data types
    if column_name.lower() == "comment":
        return "string"
    elif column_name.lower() == "time" and index == total_columns - 1:
        return "string"  # Treat time as a string
    elif column_name.lower() == "clockdatetime":
        return "timestamp"
    else:
        return "double"

# Function to generate JSON file mapping column names to data types from CSV file
def generate_column_type_mapping(input_csv_path, output_json_path, header_row_index=7):
    column_type_mapping = []
    
    # Open and read the CSV file
    with open(input_csv_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        
        # Iterate through rows
        for i, row in enumerate(reader):
            # Only process the header row
            if i == header_row_index:
                total_columns = len(row)
                
                # Create mapping for each column
                for index, column_name in enumerate(row):
                    if index < 10:
                        print(index, column_name)
                    data_type = infer_data_type(column_name, index, total_columns)
                    column_type_mapping.append({"ColumnName": column_name, "DataType": data_type})
                break

    # Write the mapping to a JSON file
    with open(output_json_path, mode='w', encoding='utf-8') as json_file:
        json.dump(column_type_mapping, json_file, indent=2)
    
    print(f"Column type mapping JSON file generated at {output_json_path}")

# Usage
input_csv_path = 'tmp.csv'  # Replace with your actual CSV file path
output_json_path = 'column_type_mapping.json'  # Replace with desired output JSON file path
generate_column_type_mapping(input_csv_path, output_json_path)
