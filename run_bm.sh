#!/bin/bash
set -e
classpath="./rocksdb/java/target/rocksdbjni-10.8.0-linux64.jar"

#
# By default run for all abort modes:
#
# 0: no conflicts
# 1: write-write conflicts (classic SI)
# 2: (refined SI)
# 3: read-write conflicts (write snapshot isolation)
#
if [ $# -eq 0 ]; then
  abort_modes="0 1 2 3"
else
  abort_modes="$1"
fi

# Rebuild Java benchmark.
rm -rf build && mkdir build
javac -cp $classpath -d ./build ZipfGenerator.java ZipfianGenerator.java Generator.java NumberGenerator.java SimpleTxnBenchmark.java 

cd build
jar cvf SimpleTxnBenchmark.jar *
cd ..

# Run for each abort mode and plot results.
for abort_mode in $abort_modes; do

    # Run benchmark.
    duration_secs=60
    jarcmd="java -cp build/SimpleTxnBenchmark.jar:./rocksdb/java/target/rocksdbjni-10.8.0-linux64.jar"
    
    # Run non-mixed workloads.
    ABORT_MODE=$abort_mode $jarcmd my.test.SimpleTxnBenchmark --duration $duration_secs
    mv benchmark_results.csv benchmark_results_${abort_mode}.csv

    ./plot_results.sh

    # Run mixed workload case as well.
    ABORT_MODE=$abort_mode $jarcmd my.test.SimpleTxnBenchmark --duration $duration_secs --mixedWorkload
    mv benchmark_results.csv benchmark_results_${abort_mode}_mixed.csv

    ./plot_results.sh

  # mv benchmark_throughput.png benchmark_throughput_abort_mode_${abort_mode}.png
done
