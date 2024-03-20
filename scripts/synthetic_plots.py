import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
plt.rcParams.update({'font.size': 8})

def create_plots(input_csv, output_pdf):
    # Read the CSV file
    df = pd.read_csv(input_csv)

    df = df.sort_values(by=["type", "workloadIterations", "selectivity_nominal"])

    x_values_dict = {1000: 0.6, 5000: 2.4, 10000: 4.7, 20000: 9.2, 40000: 18.4, 100000: 46.1}

    # Create a PDF file
    with PdfPages(output_pdf) as pdf:
        # Plot 1: Throughput vs. Workload Iterations
        plt.figure(figsize=(1.9, 2))
        for plot_type in ["NATIVE", "SINGLEOUT", "MULTIOUT"]:
            for selectivity_nominal in df['selectivity_nominal'].unique():
                subset = df[(df['type'] == plot_type) & (df['selectivity_nominal'] == selectivity_nominal)]
                plt.plot([x_values_dict[i] for i in subset['workloadIterations']], subset['throughput'], label=f"{plot_type} - Sel: {selectivity_nominal}")

        plt.xlabel(r'Load ($\mu$s)')
        plt.ylabel("Throughput")
        plt.legend()
        pdf.savefig()
        plt.close()

        # Plot 2: Latency vs. Workload Iterations
        plt.figure(figsize=(1.9, 2))
        for plot_type in ["NATIVE", "SINGLEOUT", "MULTIOUT"]:
            for selectivity_nominal in df['selectivity_nominal'].unique():
                subset = df[(df['type'] == plot_type) & (df['selectivity_nominal'] == selectivity_nominal)]
                plt.plot([x_values_dict[i] for i in subset['workloadIterations']], subset['latency'], label=f"{plot_type} - Sel: {selectivity_nominal}")

        plt.xlabel(r'Load ($\mu$s)')
        plt.ylabel("Latency")
        plt.legend()
        pdf.savefig()
        plt.close()

        # Plot 3: Throughput Percentage Difference
        
        this_scalability_fig, this_scalability_axs = plt.subplots(1,2, figsize=(3.5, 2))

        legend_symbols = {'SINGLEOUT':'A','MULTIOUT':'A\!+'}
        implementations_colors = {'SINGLEOUT': 'blue', 'MULTIOUT': 'red'}
        linestyles = ['-','--',':']

        for type in ['SINGLEOUT','MULTIOUT']:

            for i,selectivity_nominal in enumerate(df['selectivity_nominal'].unique()):
                x_values = df[(df['type'] == 'NATIVE') & (df['selectivity_nominal'] == selectivity_nominal)]['workloadIterations'].values
                native_throughput = df[(df['type'] == 'NATIVE') & (df['selectivity_nominal'] == selectivity_nominal)]['throughput'].values
                singleout_throughput = df[(df['type'] == type) & (df['selectivity_nominal'] == selectivity_nominal)]['throughput'].values
                
                percentage_diff = (singleout_throughput / native_throughput) * 100

                if selectivity_nominal.is_integer():
                    selectivity_nominal_l = int(selectivity_nominal)
                else:
                    selectivity_nominal_l = selectivity_nominal

                this_scalability_axs[0].plot([x_values_dict[i] for i in x_values], percentage_diff, 
                                             label=f'${legend_symbols[type]}\!/{selectivity_nominal_l}$',color=implementations_colors[type],
                                             linestyle=linestyles[i])
                
                print('THROUGHPUT type:',type,'Selectivity:',selectivity_nominal,'X:',[x_values_dict[i] for i in subset['workloadIterations']],'Y:',percentage_diff)


            this_scalability_axs[0].set_xlabel(r'Per-tuple proc. cost ($\mu$s)')
            this_scalability_axs[0].set_ylabel(r"Throughput (% of $D$)")
            
            # Plot 4: Latency Percentage Difference
            for i,selectivity_nominal in enumerate(df['selectivity_nominal'].unique()):
                x_values = df[(df['type'] == 'NATIVE') & (df['selectivity_nominal'] == selectivity_nominal)]['workloadIterations'].values
                native_latency = df[(df['type'] == 'NATIVE') & (df['selectivity_nominal'] == selectivity_nominal)]['latency'].values
                singleout_latency = df[(df['type'] == type) & (df['selectivity_nominal'] == selectivity_nominal)]['latency'].values
                
                percentage_diff = (abs(singleout_latency - native_latency) / native_latency) * 100
                
                if selectivity_nominal.is_integer():
                    selectivity_nominal_l = int(selectivity_nominal)
                else:
                    selectivity_nominal_l = selectivity_nominal

                this_scalability_axs[1].plot([x_values_dict[i] for i in x_values], percentage_diff, 
                                             label=f'${legend_symbols[type]}\!/{selectivity_nominal_l}$',color=implementations_colors[type],
                                             linestyle=linestyles[i])
                
                print('LATENCY type:',type,'Selectivity:',selectivity_nominal,'X:',[x_values_dict[i] for i in subset['workloadIterations']],'Y:',percentage_diff)

            this_scalability_axs[1].set_xlabel(r'Per-tuple proc. cost ($\mu$s)')
            this_scalability_axs[1].set_ylabel(r"Latency (% of D)")
            this_scalability_axs[1].set_ylim([60,5000])
            this_scalability_axs[1].set_yscale("log")
            this_scalability_axs[1].legend(ncol=1,frameon=False, borderpad=0,columnspacing=0.05,markerscale=0.5,fontsize=6.5,handlelength=2,labelspacing = 0.2)
            
        this_scalability_fig.tight_layout()
        pdf.savefig()
        plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create plots from a CSV file.")
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("output_pdf", help="Output PDF file")

    args = parser.parse_args()
    create_plots(args.input_csv, args.output_pdf)
