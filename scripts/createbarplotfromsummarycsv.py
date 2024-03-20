import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def create_bar_plots(input_file, output_pdf):
    # Read the CSV file
    df = pd.read_csv(input_file)

    # Filter the data to include only rows with outcome = 1
    df_filtered = df[df["outcome"] == 1]

    # Create a PDF file
    pdf_pages = PdfPages(output_pdf)

    # Define the metrics and plot titles
    metrics = ["injectionRate", "throughput", "latency", "cpu"]
    plot_titles = ["Injection Rate", "Throughput", "Latency", "CPU"]

    for i, metric in enumerate(metrics):
        fig, ax = plt.subplots()

        for exp_id in df["exp_id"].unique():
            exp_data = df_filtered[df_filtered["exp_id"] == exp_id]

            if exp_data.empty:
                continue

            # Find the row with the lowest sleepTime for the current exp_id
            max_throughput_row = exp_data[exp_data["throughput"] == exp_data["throughput"].max()]

            if not max_throughput_row.empty:
                value = max_throughput_row.iloc[0][metric]
                ax.bar(exp_id, value, label=exp_id)

                # Add a text annotation for the value of the bar
                ax.annotate(f"{value:.2f}", (exp_id, value), textcoords="offset points", xytext=(0,10), ha="center")


        ax.set_xlabel("exp_id")
        ax.set_ylabel(metric)
        ax.set_title(plot_titles[i])
        ax.legend()

        pdf_pages.savefig(fig)

    # Save and close the PDF
    pdf_pages.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create bar plots from a CSV file.")
    parser.add_argument("input_file", help="Input CSV file")
    parser.add_argument("output_pdf", help="Output PDF file")

    args = parser.parse_args()
    create_bar_plots(args.input_file, args.output_pdf)
