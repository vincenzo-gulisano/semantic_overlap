folder=$1
echo "merging CSVs for the same statistics in each folder that contains data..."
python scripts/summarizeexperimentstats.py ${folder}
echo "...and creating the corresponding PDFs in summary_plots.pdf and storing also average values"
python scripts/createplotsfromsummaries.py ${folder}
echo "Creating the augmented merged_log.txt"
python scripts/mergecsvs_synthetic.py ${folder}/log.txt ./ ${folder}/merged_log.txt