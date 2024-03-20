folder=data/output_files/joinwikipediamuspe/
duration=60000
batch_size=1
allowed_lateness=30000
impl_types=(NATIVE NATIVE NATIVE NATIVE NATIVE NATIVE)
exp_ids=(LLJ ALJ HLJ LHJ AHJ HHJ) 
rates=(20000 20000 20000 10000 10000 10000)
max_diffs=(500 500 500 500 500 500)

mkdir -p ${folder}/

if [ ! -f "${folder}/log.txt" ]; then
    echo "folder,exp_id,repetition,rate,sleepTime,outcome" >> ${folder}/log.txt
fi

for t in ${!impl_types[@]}; do

	type=${impl_types[$t]}

		exp_id=${exp_ids[$t]}
		echo "Starting experiments for exp_id:" $exp_id
		
		for r in {0..0}; do

			echo "Repetition " $r

			max_diff=${max_diffs[$t]}

			# The initial rate to try for a certain implementation
			rate=${rates[$t]} #3000
			maxrate=$rate
			offset=$rate

			goOn=1

			while [ $goOn -eq 1 ]
			do

				# compute new offset
				let "offset=$offset/2"

				# computing rate from sleep time
				let "sleepTime=1000000000/$rate"
				echo "Trying rate " $rate " / sleepTime " $sleepTime
				formatted_rate=$(printf "%010d" "$rate")

				exp_folder=${folder}/${exp_id}/${formatted_rate}/${r}/

				experimentAlreadyRun=0
				# Check if the experiments has already been reported
				if [ ! -z $(grep "${exp_folder},${exp_id},${r},${rate},${sleepTime},1" "${folder}/log.txt") ]; then 
					echo "This experiment has been already reported as successful!";
					return=0
					experimentAlreadyRun=1
				elif [ ! -z $(grep "${exp_folder},${exp_id},${r},${rate},${sleepTime},0" "${folder}/log.txt") ]; then 
					echo "This experiment has been already reported as not successful!";
					return=13
					experimentAlreadyRun=1
				else

					echo "Creating/cleaning folders"
					mkdir -p ${exp_folder}

					duration_seconds=$(( $duration / 1000 ))

					mvn compile exec:java -Dexec.mainClass=muSPE.JoinQuery -Dexec.args="--IP 129.16.20.20 --port 9999 --throughputOutputFile ${exp_folder}/throughput.csv --outputRateOutputFile ${exp_folder}/outputRate.csv --experimentID ${exp_id} --ack ${exp_folder}/completedsuccessfully.txt --latencyOutputFile ${exp_folder}/latency.csv --maxLatency 15000 --maxLatencyViolations 3 --duration ${duration} --sleepTime ${sleepTime} --injectionRateOutputFile ${exp_folder}/injectionRate.csv --warmup 30000 --inputFile data/input_files/insertions.tsv" > ${exp_folder}/muSPElog.txt 2>&1 &


					java_pid=$!

					echo "Java PID: ${java_pid}"

					echo "Lunching CPU monitor"
					python ./scripts/cpumonitor.py --pid ${java_pid} --output ${exp_folder}/cpu.csv
					
					if test -e ${exp_folder}/completedsuccessfully.txt; then
						echo "Experiment successful!"
						return=0
					else
						echo "Experiment not successful!"
						return=13
					fi

				fi


				if [ $return -eq 13 ]; then
					echo "The rate is not sustainable, decreasing it"
					let newRate=$rate-$offset
					let newSleepTime=1000000000/$newRate
					echo "New rate " $newRate " / sleepTime " $newSleepTime
					if [ $experimentAlreadyRun -eq 0 ]; then
						echo "${exp_folder},${exp_id},${r},${rate},${sleepTime},0" >> ${folder}/log.txt
					fi
				else 
					echo "The rate is sustainable, increasing it"
					let newRate=$rate+$offset
					let newSleepTime=1000000000/$newRate
					echo "New rate " $newRate " / sleepTime " $newSleepTime
					if [ $experimentAlreadyRun -eq 0 ]; then
						echo "${exp_folder},${exp_id},${r},${rate},${sleepTime},1" >> ${folder}/log.txt
					fi
				fi
				
				# Compute difference
				let "diff = $rate - $newRate"
				# compute absolute difference (removing initial +/-, not the best...)
				let "abs_diff = ${diff##*[+-]}"
				echo "Difference between old and new rate: " $abs_diff
				if [ $abs_diff -le $max_diff ]; then
					echo "Stopping"
					goOn=0
				else 
					if [ $newRate -ge $maxrate ]; then
						echo "The new rate exceeds the max rate, so we stop"
						goOn=0
					else 
						echo "Trying new rate"
						goOn=1 
						let rate=$newRate
					fi
				fi

			done

		done
		
done
