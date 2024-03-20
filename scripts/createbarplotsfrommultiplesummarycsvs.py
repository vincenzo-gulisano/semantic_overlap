import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np


def create_bar_plots(data_list, output_pdf, j):
    # Create a PDF file
    pdf_pages = PdfPages(output_pdf)

    # Define the metrics and plot titles
    metrics = ["throughput", "latency", "cpu"]
    plot_titles = ["Throughput", "Latency", "CPU"]
    exp_id_list = ["LLJ", "ALJ", "HLJ", "LHJ", "AHJ",
                   "HHJ", "LLF", "ALF", "HLF", "LHF", "AHF", "HHF"]
    bars_per_group = len(data_list)
    for i, metric in enumerate(metrics):
        fig, ax = plt.subplots()

        for input_file_idx, (ID, input_file) in enumerate(data_list):

            exp_ids = []
            values = []

            # Read the CSV file
            df = pd.read_csv(input_file)

            # Filter the data to include only rows with outcome = 1
            df_filtered = df[df["outcome"] == 1]

            for exp_id in exp_id_list:
                exp_data = df_filtered[df_filtered["exp_id"] == exp_id]

                if exp_data.empty:
                    continue

                # Group by 'repetition'
                grouped_data = exp_data.groupby('repetition')
                repetitions_values = []

                # Iterate over 'repetition' groups
                for repetition, group in grouped_data:

                    max_throughput_row = group.nlargest(j, "throughput")

                    if not max_throughput_row.empty:
                        # print(type(max_throughput_row.iloc[j-1][metric]))
                        repetitions_values.append(
                            max_throughput_row.iloc[j-1][metric])

                if repetitions_values:
                    average_value = sum(repetitions_values) / \
                        len(repetitions_values)
                    exp_ids.append(exp_id)
                    values.append(average_value)

            # Generate X values
            x = [x*(bars_per_group+1) +
                 input_file_idx for x in np.arange(len(exp_ids))]

            print("x", x)

            ax.bar(x, values, label=ID)
            # Add a text annotation for the value of the bar
            for x_i, value in enumerate(values):
                ax.annotate(f"{value:.2f}", (x[x_i], value), textcoords="offset points", xytext=(0, 10), ha="center",rotation=90,fontsize=8)

        ax.set_xticks([x*(bars_per_group+1)+(len(data_list)-1) /
                      2 for x in np.arange(len(exp_ids))])
        ax.set_xticklabels(exp_ids)

        ax.set_xlabel("exp_id")
        ax.set_ylabel(metric)
        ax.set_title(plot_titles[i])
        ax.legend()

        pdf_pages.savefig(fig)

    # Save and close the PDF
    pdf_pages.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create bar plots from a list of ID-CSV file pairs.")
    parser.add_argument("data_list", nargs='+',
                        help="List of ID-CSV file pairs")
    parser.add_argument("output_pdf", help="Output PDF file")
    parser.add_argument("i", help="i-th highest throughput")

    args = parser.parse_args()

    data_list = [(item.split('-')[0], item.split('-')[1])
                 for item in args.data_list]
    create_bar_plots(data_list, args.output_pdf, int(args.i))
