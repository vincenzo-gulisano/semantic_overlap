folder=$1
python scripts/summarizeexperimentstats.py ${folder}
python scripts/createplotsfromsummariesofexperiments.py ${folder}
python scripts/mergecsvs.py ${folder}/log.txt ./ ${folder}/merged_log.txt
python scripts/createplotfrommergedfile.py ${folder}/merged_log.txt ${folder}/merged_log.pdf
python scripts/createplotsforvariousinjectionrates.py ${folder}/merged_log.txt ${folder}/overtimegraphs.pdf
python scripts/createbarplotfromsummarycsv.py ${folder}/merged_log.txt ${folder}/bars.pdf