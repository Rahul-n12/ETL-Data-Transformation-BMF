import pandas as pd
import re
import yaml

# Load the input data from the Excel file
input_data = pd.read_excel("C:/Users/rahul/Downloads/project_ETL/project_ETL/input/input.xlsx", engine="openpyxl")

# Define the transformation functions
def insert(row, source, target):
    row[target] = row[source]
    return row

def constant(row, target, value):
    row[target] = value
    return row

def replace_substring(row, source, target, old, new, regex=False):
    if row[source] is not None:
        if regex:
            row[target] = re.sub(old, new, str(row[source]))
        else:
            row[target] = str(row[source]).replace(old, new)
    return row

def key_value_translate(row, source, target, translate_dict):
    if row[source] in translate_dict:
        row[target] = translate_dict[row[source]]
    return row

# Define the transformation dictionary
transformation_functions = {
    'insert': insert,
    'constant': constant,
    'replace_substring': replace_substring,
    'key_value_translate': key_value_translate
}

# Read the transformation configuration from YAML
with open("C:/Users/rahul/Downloads/project_ETL/project_ETL/transformation_config.yml", "r") as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)

# Apply transformations based on the configuration
for transformation in config['transformations']:
    action = transformation['action']
    target = transformation.get('target')

    if action in transformation_functions:
        function = transformation_functions[action]

        # Check if the function accepts the 'action' parameter
        if 'action' in function.__code__.co_varnames:
            # Include 'action' in the transformation parameters
            input_data = input_data.apply(function, axis=1, **transformation)
        else:
            # Exclude 'action' from the transformation parameters
            transformation_without_action = {k: v for k, v in transformation.items() if k != 'action'}
            input_data = input_data.apply(function, axis=1, **transformation_without_action)
    else:
        print(f"Transformation action '{action}' not found in transformation_functions.")



# Only keep the target columns in the output
unique_target_columns = input_data.columns.difference(["RecordId", "DocNumber", "DocVersion"])
#output_data = input_data[unique_target_columns]
output_data = input_data[unique_target_columns]

# Include 'file' column in the output
output_data.loc[:, 'file'] = input_data['FileLocation']

# Assuming we have the 'output_data' DataFrame ready
output_data = output_data.copy()  # Create a copy
output_data['document_number'] = output_data['file'].str.extract(r'Doc(\d+\.\d+)')


document_numbers = []

for file_path in output_data['file']:
    # Split the path by backslash (\) and get the second-to-last part
    parts = file_path.split("\\")
    if len(parts) >= 2:
        document_number = parts[-2]
    else:
        document_number = "Missing"  # Add "Missing" for missing entries
    document_numbers.append(document_number)

# Print the extracted document numbers
formatted_output = []

for input_str in document_numbers:
    # If the document number is "Missing," add it as-is; otherwise, remove the "Doc" prefix and replace underscore (_) with a dot (.)
    if input_str == "Missing":
        formatted_output.append("Missing")
    else:
        formatted_output.append(input_str.replace("Doc", "").replace("_", "."))

# Ensure that formatted_output matches the number of rows in the DataFrame
if len(formatted_output) == len(output_data):
    output_data['document_number'] = formatted_output
else:
    print("Length of formatted_output does not match the number of rows in the DataFrame.")

# Reorder and rename the columns
output_data['Validated?'] = output_data['Validated?'].apply(lambda x: "TRUE" if x == "Yes" else "FALSE")
output_data = output_data[['file', 'Validated?', 'source', 'document_number', 'major_version__v']]
output_data.columns = ['file', 'validated', 'source', 'document_number', 'major_version__v']

# Ensure the DataFrame has all necessary columns
required_columns = ['file', 'validated', 'source', 'document_number', 'major_version__v']
for col in required_columns:
    if col not in output_data.columns:
        print(f"Warning: Missing column {col} in output_data")

# Display the DataFrame in the terminal
print('output_data is below', output_data)

print(output_data.head())
print(output_data.columns)

# Save the transformed data to a CSV file
output_data.to_csv("C:/Users/rahul/Downloads/project_ETL/project_ETL/outputs/output.csv", index=False)
