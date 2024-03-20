import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from PyPDF2 import PdfFileWriter, PdfFileReader
from io import BytesIO


def create_plots_and_pdf(parent_folder):

    # Iterate over all directories and subdirectories in the parent folder
    for root, dirs, files in os.walk(parent_folder):

        # Dictionary to store average values for each summary file
        avg_values = {}

        for folder in dirs:
            folder_path = os.path.join(root, folder)
            summary_files = [f for f in os.listdir(
                folder_path) if f.startswith("summary.") and f.endswith(".csv")]

            if not summary_files:
                continue  # No summary files found in this folder

            # Initialize the PDF document for this folder
            pdf = PdfPages(os.path.join(folder_path, 'summary_plots.pdf'))

            min_timestamp = None  # To track the minimum timestamp for alignment

            # Iterate over the summary files in this folder
            for summary_file in summary_files:
                summary_file_path = os.path.join(folder_path, summary_file)
                df = pd.read_csv(summary_file_path)
                timestamp = df['timestamp']

                # Find the minimum timestamp for alignment
                if min_timestamp is None:
                    min_timestamp = min(timestamp)
                else:
                    min_timestamp = min(min_timestamp, min(timestamp))

            # Iterate over the summary files in this folder
            for summary_file in summary_files:
                summary_file_path = os.path.join(folder_path, summary_file)
                df = pd.read_csv(summary_file_path)
                timestamp = df['timestamp']
                value = df['value']

                # Calculate the average value excluding first/last 10% and -1 values
                valid_indices = (timestamp >= timestamp.quantile(0.10)) & (
                    timestamp <= timestamp.quantile(0.90))
                valid_indices = valid_indices & (value != -1)
                avg_value = value[valid_indices].mean()

                # Store the average value in the dictionary
                avg_values[summary_file] = avg_value

                # Create the plot
                plt.figure()
                plt.plot(timestamp - min_timestamp, value, label="Value")
                plt.axhline(avg_value, color='red', linestyle='--',
                            label=f"Average: {avg_value:.2f}")
                plt.xlabel('Aligned Timestamp')
                plt.ylabel('Value')
                plt.title(f"{summary_file} - Average: {avg_value:.2f}")
                plt.legend()

                # Add the plot to the PDF
                pdf.savefig(plt.gcf(), bbox_inches='tight')

                # Close the plot
                plt.close()

            # Close the PDF document
            pdf.close()

            # print(f"PDF report created for folder: {folder_path}")

            # Create a DataFrame from the average values dictionary and save it to a CSV
            avg_values_df = pd.DataFrame.from_dict(
                avg_values, orient='index')
            avg_values_df.index = avg_values_df.index.str.replace(
                "summary.", "").str.replace(".csv", "")
            avg_values_df = avg_values_df.transpose()  # Transpose the data
            avg_values_df.to_csv(os.path.join(
                folder_path, 'summary_avg_values.csv'), index=False, header=True)


def main():
    parser = argparse.ArgumentParser(
        description="Create plots and a PDF report from summary files.")
    parser.add_argument("parent_folder", help="Path to the parent folder")
    args = parser.parse_args()

    create_plots_and_pdf(args.parent_folder)


if __name__ == "__main__":
    main()
