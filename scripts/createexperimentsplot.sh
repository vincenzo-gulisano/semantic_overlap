folder=$1
echo "merging CSVs for the same statistics in each folder that contains data..."
python scripts/summarizesparkexperimentstats.py ${folder}
echo "...and creating the corresponding PDFs in summary_plots.pdf and storing also average values"
python scripts/createplotsfromsummaries.py ${folder}
echo "Creating the augmented merged_log.txt"
python scripts/mergecsvs.py ${folder}/log.txt ./ ${folder}/merged_log.txt
echo "Create graphs over injection rate"
python scripts/createplotsforvariousinjectionrates.py ${folder}/merged_log.txt ${folder}/overtimegraphs.pdf
echo "Create bar plots"
python scripts/createbarplotfromsummarycsv.py ${folder}/merged_log.txt ${folder}/bars.pdf 3