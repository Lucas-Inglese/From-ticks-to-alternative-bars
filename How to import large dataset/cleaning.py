import os
import pandas as pd
from tqdm import tqdm

# Define the root folder
root_folder = r'C:\Users\Oliver\Desktop\admiral\DATA\USDJPY-Admiral-Markets'

# Create a list to store dictionaries of file information
file_info_list = []

# Function to process each Parquet file
def process_parquet(file_path):
    # Load the parquet file
    df = pd.read_parquet(file_path)

    # Check for missing values
    missing_values_count = df.isnull().sum().sum()

    # Count duplicates
    duplicate_count = df.duplicated().sum()

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Save changes to the same file
    df.to_parquet(file_path, compression='gzip')

    # Extract relevant parts of file path for index
    parts = file_path.split(os.sep)
    index = os.sep.join(parts[-3:])  # Take the last three parts (file name and two subfolders)

    # Construct dictionary for file information
    file_info = {
        'File Name': parts[-1],  # File name
        'Subfolder 1': parts[-3],  # First subfolder
        'Subfolder 2': parts[-2],  # Second subfolder
        'Missing Values': missing_values_count,
        'Duplicates Deleted': duplicate_count
    }

    # Append file information to the list
    file_info_list.append(file_info)

# Walk through each folder and subfolder
for folder_path, _, files in os.walk(root_folder):
    for file in tqdm(files, desc=f"Processing {folder_path}"):
        if file.endswith('.parquet'):
            file_path = os.path.join(folder_path, file)
            process_parquet(file_path)

# Create a DataFrame from the list of file information dictionaries
file_info_df = pd.DataFrame(file_info_list)

# Set the index to file path and keep only the file name and the next two subfolders
file_info_df.set_index('File Name', inplace=True)
file_info_df.index.name = None  # Remove the name of the index
file_info_df = file_info_df[['Subfolder 1', 'Subfolder 2', 'Missing Values', 'Duplicates Deleted']]

# Print the DataFrame
print(file_info_df)
