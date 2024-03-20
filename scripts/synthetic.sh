folder=data/output_files/synthetic/
duration=120000
allowed_lateness=30000
impl_types=(NATIVE SINGLEOUT MULTIOUT)
rates=(10000000 10000000 10000000)
max_diffs=(5000 5000 5000)
workloadIterationss=(1000 5000 10000 20000 40000 100000)
selectivities=(0.1 1 3)

mkdir -p ${folder}/

if [ ! -f "${folder}/log.txt" ]; then
    echo "folder,type,repetition,workloadIterations,selectivity_nominal,rate,sleepTime,outcome" >> ${folder}/log.txt
fi

for t in ${!impl_types[@]}; do

	type=${impl_types[$t]}

	for w in ${!workloadIterationss[@]}; do

		workloadIterations=${workloadIterationss[$w]}

		echo "workloadIterations " $workloadIterations

		for s in ${!selectivities[@]}; do

			selectivity=${selectivities[$s]}

			echo "selectivity " $selectivity

			for r in {0..0}; do

				echo "Repetition " $r

				# The initial rate to try for a certain implementation
				rate=${rates[$t]}
				max_diff=${max_diffs[$t]}
				maxrate=$rate
				offset=$rate

				goOn=1

				while [ $goOn -eq 1 ]
				do

					# compute new offset
					let "offset=$offset/2"

					# computing rate from sleep time
					formatted_rate=$(printf "%010d" "$rate")
					let "sleepTime=1000000000/$rate"
					echo "Trying rate " $formatted_rate " / sleepTime " $sleepTime
						
					exp_folder=${folder}/${type}/${workloadIterations}/${selectivity}/${formatted_rate}/${r}/

					experimentAlreadyRun=0
					# Check if the experiments has already been reported
					if [ ! -z $(grep "${exp_folder},${type},${r},${workloadIterations},${selectivity},${formatted_rate},${sleepTime},1" "${folder}/log.txt") ]; then 
						echo "This experiment has been already reported as successful!";
						return=0
						experimentAlreadyRun=1
						# sleep 5
					elif [ ! -z $(grep "${exp_folder},${type},${r},${workloadIterations},${selectivity},${formatted_rate},${sleepTime},0" "${folder}/log.txt") ]; then 
						echo "This experiment has been already reported as not successful!";
						return=13
						experimentAlreadyRun=1
						# sleep 5
					else

						echo "Creating/cleaning folders"
						mkdir -p ${exp_folder}

						duration_seconds=$(( $duration / 1000 ))

						# Create classpath variable
						mvn dependency:build-classpath -Dmdep.outputFile=tmp.txt
						CLASSPATH=$(cat tmp.txt)
						java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -classpath ./target/aggregates_for_the_win-1.0-SNAPSHOT.jar:$CLASSPATH usecase.synthetic.QueryFlatMap --injectionRateOutputFile ${exp_folder}/injectionRate.csv --throughputOutputFile ${exp_folder}/throughput.csv --latencyOutputFile ${exp_folder}/latency.csv --duration ${duration} --sleepTime ${sleepTime} --outputRateOutputFile ${exp_folder}/outputRate.csv --maxLatencyViolations 3 --maxLatency 15000 --implementationType ${type} --workloadIterations ${workloadIterations} --selectivity ${selectivity}

						if test -e ${exp_folder}/latencyexception.txt; then
							echo "Experiment not successful!"
							return=13
						else
							echo "Experiment successful!"
							return=0
						fi

					fi

					if [ $return -eq 13 ]; then
						echo "The rate is not sustainable, decreasing it"
						let newRate=$rate-$offset
						let newSleepTime=1000000000/$newRate
						echo "New rate " $newRate " / sleepTime " $newSleepTime
						if [ $experimentAlreadyRun -eq 0 ]; then
							echo "${exp_folder},${type},${r},${workloadIterations},${selectivity},${formatted_rate},${sleepTime},0" >> ${folder}/log.txt
						fi
					else 
						echo "The rate is sustainable, increasing it"
						let newRate=$rate+$offset
						let newSleepTime=1000000000/$newRate
						echo "New rate " $newRate " / sleepTime " $newSleepTime
						if [ $experimentAlreadyRun -eq 0 ]; then
							echo "${exp_folder},${type},${r},${workloadIterations},${selectivity},${formatted_rate},${sleepTime},1" >> ${folder}/log.txt
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

	done

done