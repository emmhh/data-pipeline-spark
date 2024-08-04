import csv
import json
from collections import defaultdict

def map_columns_and_save_to_json(csv_file, json_file):
    # Open the CSV file
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        
        # Read the first 8 lines
        rows = [next(reader) for _ in range(8)]
    
    # Get row 7 and row 8
    row_7 = rows[6]
    row_8 = rows[7]

    # Initialize mapping dictionary
    mapping = defaultdict(list)

    # Add the first two columns directly
    mapping[row_8[0].strip()].append(row_8[0].strip())
    mapping[row_8[1].strip()].append(row_8[1].strip())

    previous_super_category = None

    # Iterate over each column starting from the third column
    for i in range(2, len(row_8)):
        if row_7[i].strip():
            previous_super_category = row_7[i].strip()
            mapping[previous_super_category].append(row_8[i].strip())
        else:
            mapping[previous_super_category].append(row_8[i].strip())

    # Convert the mapping to a list of dictionaries for JSON
    mapping_list = [{"SuperCategory": key, "ColumnNames": value} for key, value in mapping.items()]

    # Write the mapping to a JSON file
    with open(json_file, 'w') as json_out:
        json.dump(mapping_list, json_out, indent=2)
    
    print(f"Mapping saved to {json_file}")

# Usage example
csv_file_path = "tmp.csv"
json_file_path = "mapping.json"
map_columns_and_save_to_json(csv_file_path, json_file_path)
