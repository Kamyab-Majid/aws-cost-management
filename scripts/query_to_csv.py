import os

import pandas as pd

# Step 1: Read the CSV file into a DataFrame
csv_file_path = "athena_result.csv"
df = pd.read_csv(csv_file_path)

# Step 2: Get a list of file names in the folder
folder_path = "queries"
file_names = os.listdir(folder_path)

# Step 3: Iterate through each file and update the DataFrame
for file_name in file_names:
    file_path = os.path.join(folder_path, file_name)

    # Extract query_id from the file name (assuming the file names are in the format "query_id.txt")
    query_id = os.path.splitext(file_name)[0]

    # Read the content of the file
    with open(file_path, "r") as file:
        content = file.read()

    # Update the corresponding row in the DataFrame
    df.loc[df["ID"] == (query_id), "query"] = content
# Step 4: Write the updated DataFrame back to the CSV file
df.to_parquet("highest_cost.parquet", index=False)
df.to_excel("example.xlsx")
