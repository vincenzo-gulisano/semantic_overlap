import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import math
import warnings
plt.rcParams.update({'font.size': 8})
plt.rcParams["legend.markerscale"] = 0.3
import matplotlib.patches as mpatches


def create_bar_plots_for_paper_flatmap_throughput():

    experiments_flatmap = ["LLM", "ALM", "HLM", "LHM", "AHM", "HHM"]
    throughput_flink = [326.107,316.382,315.256,147.847,152.542,147.851]
    throughput_spark = [467.154,348.140,263.790,222.916,193.895,167.393]
    throughput_uspe = [512.563,465.130,441.317,233.542,207.689,181.09924]
    barplot_fig, barplot_axs = plt.subplots(1, 1, figsize=(1.9, 1.6))
    barplot_axs.grid(True, axis='y')
    x = np.arange(len(experiments_flatmap))  # the label locations
    width = 0.333  # the width of the bars
    gap = 0.25

    rects1 = barplot_axs.bar(x - width + x*gap, throughput_flink, width, label='Flink', color='black')
    rects2 = barplot_axs.bar(x + x*gap, throughput_spark, width, label='Spark', color='blue')
    rects3 = barplot_axs.bar(x + width + x*gap, throughput_uspe, width, label=r'$\mu$SPE', color='red')

    # Add some text for labels, title and custom x-axis tick labels, etc.

    barplot_axs.set_xticks(x + x*gap, experiments_flatmap,rotation=90)
    barplot_axs.set_ylabel(r'Throughput ($10^3$ t/s)', loc='top')

    barplot_axs.legend(ncol=3,frameon=False, borderpad=0,columnspacing=0.2,markerscale=0.5,loc='upper center', bbox_to_anchor=(0.3, 1.2),fontsize=7.5)
    
    barplot_fig.tight_layout()
    plt.gca().set_axisbelow(True)
    barplot_fig.savefig('./jupyter/data/interspe-flatmap-throughput.pdf')


def create_bar_plots_for_paper_flatmap_latency():

    experiments_flatmap = ["LLM", "ALM", "HLM", "LHM", "AHM", "HHM"]
    latency_flink = [0.09158,0.01629,0.16667,0.05772,0.10724,0.02947]
    latency_spark = [0.62761,1.80163,0.28733,0.37792, 1.08373, 0.49760]
    latency_uspe = [0.02607,0.02228,0.01496, 0.09735, 0.01990, 0.10811]
    barplot_fig, barplot_axs = plt.subplots(1, 1, figsize=(1.9, 1.6))
    barplot_axs.grid(True, axis='y')
    x = np.arange(len(experiments_flatmap))  # the label locations
    width = 0.333  # the width of the bars
    gap = 0.25

    rects1 = barplot_axs.bar(x - width + x*gap, latency_flink, width, label='Flink', color='black')
    rects2 = barplot_axs.bar(x + x*gap, latency_spark, width, label='Spark', color='blue')
    rects3 = barplot_axs.bar(x + width + x*gap, latency_uspe, width, label=r'$\mu$SPE', color='red')

    # Add some text for labels, title and custom x-axis tick labels, etc.

    barplot_axs.set_xticks(x + x*gap, experiments_flatmap,rotation=90)
    barplot_axs.set_ylabel('Latency (s)')

    barplot_axs.legend(ncol=3,frameon=False, borderpad=0,columnspacing=0.2,markerscale=0.5,loc='upper center', bbox_to_anchor=(0.3, 1.2),fontsize=7.5)
    
    barplot_fig.tight_layout()
    plt.gca().set_axisbelow(True)
    barplot_fig.savefig('./jupyter/data/interspe-flatmap-latency.pdf')


def create_bar_plots_for_paper_join_throughput():

    experiments_flatmap = ["LLJ", "ALJ", "HLJ", "LHJ", "AHJ", "HHJ"]
    throughput_flink = [20.37048873, 21.22089554, 20.12622615, 19.95012565, 19.95967909, 20.29423415]
    throughput_spark = [2.57117670, 2.50251894, 2.41754978, 2.69129963, 2.63939742, 2.61227758]
    throughput_uspe = [58.96551762, 49.35159971, 45.27114762, 52.96387068, 58.31641817, 47.39431631]
    barplot_fig, barplot_axs = plt.subplots(1, 1, figsize=(1.9, 1.6))
    barplot_axs.grid(True, axis='y')
    x = np.arange(len(experiments_flatmap))  # the label locations
    width = 0.333  # the width of the bars
    gap = 0.25

    rects1 = barplot_axs.bar(x - width + x*gap, throughput_flink, width, label='Flink', color='black')
    rects2 = barplot_axs.bar(x + x*gap, throughput_spark, width, label='Spark', color='blue')
    rects3 = barplot_axs.bar(x + width + x*gap, throughput_uspe, width, label=r'$\mu$SPE', color='red')

    # Add some text for labels, title and custom x-axis tick labels, etc.

    barplot_axs.set_xticks(x + x*gap, experiments_flatmap,rotation=90)
    barplot_axs.set_ylabel(r'Throughput ($10^6$ c/s)', loc='top')

    barplot_axs.legend(ncol=3,frameon=False, borderpad=0,columnspacing=0.2,markerscale=0.5,loc='upper center', bbox_to_anchor=(0.3, 1.2),fontsize=7.5)
    
    barplot_fig.tight_layout()
    plt.gca().set_axisbelow(True)
    barplot_fig.savefig('./jupyter/data/interspe-join-throughput.pdf')


def create_bar_plots_for_paper_join_latency():

    experiments_flatmap = ["LLJ", "ALJ", "HLJ", "LHJ", "AHJ", "HHJ"]
    latency_flink = [4.91247, 5.42190, 3.51991, 8.45215, 8.18619, 9.28024]
    latency_spark = [27.44873, 27.52698, 27.04713, 26.60558, 28.66392, 28.44388]
    latency_uspe = [4.30467, 3.65966, 4.76620, 10.39346, 10.53943, 12.09261]
    barplot_fig, barplot_axs = plt.subplots(1, 1, figsize=(1.9, 1.6))
    barplot_axs.grid(True, axis='y')
    x = np.arange(len(experiments_flatmap))  # the label locations
    width = 0.333  # the width of the bars
    gap = 0.25

    rects1 = barplot_axs.bar(x - width + x*gap, latency_flink, width, label='Flink', color='black')
    rects2 = barplot_axs.bar(x + x*gap, latency_spark, width, label='Spark', color='blue')
    rects3 = barplot_axs.bar(x + width + x*gap, latency_uspe, width, label=r'$\mu$SPE', color='red')

    # Add some text for labels, title and custom x-axis tick labels, etc.

    barplot_axs.set_xticks(x + x*gap, experiments_flatmap,rotation=90)
    barplot_axs.set_ylabel('Latency (s)')

    barplot_axs.legend(ncol=3,frameon=False, borderpad=0,columnspacing=0.2,markerscale=0.5,loc='upper center', bbox_to_anchor=(0.3, 1.2),fontsize=7.5)
    
    barplot_fig.tight_layout()
    plt.gca().set_axisbelow(True)
    barplot_fig.savefig('./jupyter/data/interspe-join-latency.pdf')

create_bar_plots_for_paper_flatmap_throughput()
create_bar_plots_for_paper_flatmap_latency()
create_bar_plots_for_paper_join_throughput()
create_bar_plots_for_paper_join_latency()