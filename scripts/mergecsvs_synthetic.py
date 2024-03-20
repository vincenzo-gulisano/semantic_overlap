import argparse
import pandas as pd

def update_csv(input_csv, base_folder, output_csv):
    # Read the input CSV
    df = pd.read_csv(input_csv)

    # Iterate over each row in the input CSV
    for index, row in df.iterrows():
        folder = row['folder']
        summary_csv_path = f"{base_folder}/{folder}/summary_avg_values.csv"

        print('merging',summary_csv_path)

        try:
            # Read the summary_avg_values.csv for the current folder
            summary_df = pd.read_csv(summary_csv_path)

            # Update the corresponding row in the input CSV with values
            df.at[index, 'outputRate'] = summary_df['outputRate'].values[0]
            df.at[index, 'throughput'] = summary_df['throughput'].values[0]
            df.at[index, 'latency'] = summary_df['latency'].values[0]
            df.at[index, 'injectionRate'] = summary_df['injectionRate'].values[0]
            # df.at[index, 'cpu'] = summary_df['cpu'].values[0]
            df.at[index, 'selected'] = 0

        except FileNotFoundError:
            print(f"File not found for folder: {folder}")

    # Save the updated CSV
    df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update CSV with values from summary_avg_values.csv')
    parser.add_argument('input_csv', type=str, help='Path to the input CSV file')
    parser.add_argument('base_folder', type=str, help='Path to the base folder')
    parser.add_argument("output_csv", help="Path to the output CSV file")

    args = parser.parse_args()
    update_csv(args.input_csv, args.base_folder, args.output_csv)
