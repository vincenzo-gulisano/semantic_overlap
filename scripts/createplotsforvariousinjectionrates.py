import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def generate_plots(input_file, output_pdf):
    df = pd.read_csv(input_file)

    # Filter the rows with outcome = 1
    df = df[df['outcome'] == 1]

    # Get unique exp_id values
    unique_exp_ids = df['exp_id'].unique()

    with PdfPages(output_pdf) as pdf:
        for exp_id in unique_exp_ids:
            exp_data = df[df['exp_id'] == exp_id]

            # Sort data by injectionRate
            exp_data = exp_data.sort_values(by='rate')
            # Resetting indexes
            exp_data.reset_index(drop=True, inplace=True)
            max_injection_rate_index = exp_data['rate'].idxmax()

            fig, axs = plt.subplots(4, 1, figsize=(10, 10), sharex=True)
            fig.suptitle(f'exp_id: {exp_id}')

            for i, (y_column, ylabel) in enumerate(
                [('injectionRate', 'Injection Rate'), ('throughput', 'Throughput'), ('latency', 'Latency'), ('cpu', 'CPU')]
            ):
                ax = axs[i]
                ax.plot(exp_data['rate'], exp_data[y_column], marker='o', linestyle='-', markersize=5)
                ax.set_ylabel(ylabel)

                # Annotate points
                for j, (x, y) in enumerate(zip(exp_data['rate'], exp_data[y_column])):
                    offset = j - max_injection_rate_index
                    ax.annotate(f'{offset}', (x, y), textcoords="offset points", xytext=(0, 10))

            axs[-1].set_xlabel('Injection Rate')

            pdf.savefig(fig)
            plt.close(fig)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate PDF plots from a CSV file.')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('output_pdf', help='Path to the output PDF file')

    args = parser.parse_args()
    generate_plots(args.input_file, args.output_pdf)
