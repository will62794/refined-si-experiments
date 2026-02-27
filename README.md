# Refined Snapshot Isolation

Implementation and benchmarking code for refined snapshot isolation experiments.

## Setup

To initialize submodules (`rocksdb` and `elle`), run:

```bash
git submodule update --init --recursive
```

To build RocksDB with Java bindings, run:

```bash
./buildrocks.sh <NUM_COMPILATION_WORKERS>
```

and to build new standalone Elle JAR, first make sure you have [Leiningen](https://leiningen.org/) installed, and then run:

```bash
./buildelle.sh      
```

## Running benchmarks

To run performance benchmarks that measure throughput and abort rates of isolation levels under contention:
```
./run_bm.sh
```
which will run the benchmarks for all abort modes (0: no conflicts, 1: write-write conflicts (classic SI), 2: (refined SI), 3: read-write conflicts (write snapshot isolation)) and plot the results in `benchmark_throughput.png` and `benchmark_throughput_mixed.png` for the mixed workload case.

To run isolation level verification with Elle, run:
```
./run_elle.sh
```
which will run the verification for all abort modes and record the results, in terms of what anomalies are discovered for each of the above isolation level implementations in the `elle-results/` directory.