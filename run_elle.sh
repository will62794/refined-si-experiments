#!/bin/bash

classpath="./rocksdb/java/target/rocksdbjni-10.8.0-linux64.jar"
DB_DIR="dbdata"

#
# Abort modes:
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

# Map abort modes to string labels
declare -A abort_mode_labels
abort_mode_labels=(
  [0]="atomic-snapshot"
  [1]="si-classic"
  [2]="si-refined"
  [3]="write-snapshot-isolation"
)


# Rebuild Java benchmark.
rm -rf build && mkdir build
javac -cp $classpath -d ./build ZipfGenerator.java ZipfianGenerator.java Generator.java NumberGenerator.java SimpleTxnBenchmark.java 

cd build
jar cvf SimpleTxnBenchmark.jar *
cd ..

# ELLE_CLI_JAR=/home/ec2-user/elle-cli/target/elle-cli-0.1.9-standalone.jar
ELLE_CLI_JAR=elle/target/elle-0.2.7-standalone.jar

rm -rf elle-results && mkdir elle-results

MAX_TRANSACTIONS=1000000

#
# Check specified abort modes.
#
for abort_mode in $abort_modes; do

    echo "Checking with Elle for abort mode: ${abort_mode_labels[$abort_mode]}"

    # Clean up dbdata directory.
    rm -rf $DB_DIR &&mkdir $DB_DIR

    # Run benchmark.
    duration_secs=30
    jarcmd="java -cp build/SimpleTxnBenchmark.jar:./rocksdb/java/target/rocksdbjni-10.8.0-linux64.jar"

    LOG_WRITE_VERSIONS=1 ABORT_MODE=$abort_mode $jarcmd my.test.SimpleTxnBenchmark --duration $duration_secs --noWarmup --maxTransactions $MAX_TRANSACTIONS --mixedWorkload --jepsenEventModelType RWRegister | tee bmrocks.log

    # Augment RocksDB event log with version info.
    start_time=$(date +%s)

    python3 elle_hist_process.py

    # Check isolation.
    echo "Checking isolation levels."
    label=${abort_mode_labels[$abort_mode]}
    java -jar $ELLE_CLI_JAR -p png -d elle-results/${label}_snapshot-isolation -v -c snapshot-isolation --model rw-register event_log_with_versions.json | tee elle-results/${label}_snapshot-isolation.out
    java -jar $ELLE_CLI_JAR -p png -d elle-results/${label}_serializable -v -c serializable --model rw-register event_log_with_versions.json | tee elle-results/${label}_serializable.out

    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    echo "Total time to check isolation for abort mode ${abort_mode_labels[$abort_mode]}: ${elapsed} seconds"

done
