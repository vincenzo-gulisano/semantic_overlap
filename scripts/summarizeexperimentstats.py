import os
import argparse
import pandas as pd

# Define the column names
column_names = ["timestamp", "value"]


def is_csv_file_empty(file_path):
    try:
        # Attempt to read the CSV file with pandas
        df = pd.read_csv(file_path, names=column_names)

        # Check if the DataFrame is empty
        if df.empty:
            return True
        else:
            return False
    except pd.errors.EmptyDataError:
        # Handle the case where the file is empty (contains no data)
        return True
    except Exception as e:
        # Handle other potential errors (e.g., file not found)
        print(f"Error reading file: {e}")
        return False


def summarize_data(parent_folder, prefix_operations):

    # Iterate over all directories and subdirectories in the parent folder
    for root, dirs, files in os.walk(parent_folder):

        print('root:',root,'dirs:',dirs)

        for folder in dirs:
            folder_path = os.path.join(root, folder)

            # Initialize a dictionary to store data for each prefix
            prefix_data = {}

            # Iterate over the prefix-operation pairs
            for prefix, operation in prefix_operations:

                print('Checking folder', folder_path)
                print('Checking prefix', prefix,'operation',operation)
                print('Files', os.listdir(folder_path))

                prefix_files = [f for f in os.listdir(
                    folder_path) if f.startswith(prefix)]

                print('Matching Files', prefix_files)

                if len(prefix_files) == 0:
                    print(
                        f"No files found for prefix: {prefix} in folder {folder_path}")
                    continue

                # Initialize a dictionary to store data for the current prefix
                prefix_data[prefix] = []

                # Iterate over the files with the current prefix
                for filename in prefix_files:
                    file_path = os.path.join(folder_path, filename)

                    print('Reading file', file_path)

                    if not is_csv_file_empty(file_path):
                        df = pd.read_csv(file_path, names=column_names)

                        if operation == "sum":
                            grouped = df.groupby("timestamp")["value"].sum()
                        elif operation == "avg":
                            grouped = df.groupby("timestamp")["value"].apply(
                                lambda x: -1 if all(x == -1) else x[x != -1].mean())
                        else:
                            print(
                                f"Unsupported operation: {operation} for prefix {prefix}")
                            continue

                        prefix_data[prefix].append(grouped)
                        print('grouped\n', grouped)

                    else:
                        print('Skipping file because it is empty')

                # # Combine data for each prefix
                combined_data = {}
                # for prefix, data_list in prefix_data.items():

                print('Entering combining loop')

                if len(prefix_data[prefix]) > 0:
                    if operation == "sum":
                        combined_data[prefix] = pd.concat(
                            prefix_data[prefix]).groupby("timestamp").sum()
                    elif operation == "avg":
                        combined_data[prefix] = pd.concat(prefix_data[prefix]).groupby("timestamp").apply(
                            lambda x: -1 if all(x == -1) else x[x != -1].mean())
                    else:
                        print(
                            f"Unsupported operation: {operation} for prefix {prefix}")
                        continue

                    print('prefix\n', prefix, '\n', combined_data[prefix])

                    print('About to write files')
                    # Create a summary file for each prefix in the folder
                    # for prefix, summary in combined_data.items():
                    summary_filename = os.path.join(
                        folder_path, f"summary.{prefix}.csv")
                    
                    print('for\n', prefix, 'writing\n', combined_data[prefix],'in\n',summary_filename)
                    combined_data[prefix].reset_index().to_csv(summary_filename, index=False)
                    print(
                        f"Summary file created for prefix {prefix} in folder {folder_path}: {summary_filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Summarize data from CSV files with prefixes and operations.")
    parser.add_argument("parent_folder", help="Path to the parent folder")
    args = parser.parse_args()

    # Define the prefix-operation pairs
    prefix_operations = [["outputRate", "sum"], ["throughput", "sum"], [
        "latency", "avg"], ["injectionRate", "sum"],["cpu", "sum"]]

    summarize_data(args.parent_folder, prefix_operations)


if __name__ == "__main__":
    main()
