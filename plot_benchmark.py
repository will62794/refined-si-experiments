#!/usr/bin/env python3
"""
Script to plot throughput vs number of clients from RocksDB benchmark results.
"""

import matplotlib.pyplot as plt
import numpy as np
import argparse
import os
import csv
import matplotlib.cm as cm

def load_benchmark_data(csv_file):
    """Load benchmark data from CSV file using plain Python csv module."""
    try:
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            data = []
            for row in reader:
                # Skip rows that are completely empty
                if all(x is None or x.strip() == "" for x in row.values()):
                    continue
                # Convert columns to appropriate dtypes
                processed = {}
                for k, v in row.items():
                    if k in ['clients', 'keys_per_txn', 'keyspace_size']:
                        try:
                            processed[k] = int(v)
                        except Exception:
                            processed[k] = 0
                    elif k in ['transactions_per_sec', 'operations_per_sec',
                               'mean_latency_ms', 'success_rate', 'read_ratio']:
                        try:
                            processed[k] = float(v)
                        except Exception:
                            processed[k] = 0.0
                    else:
                        processed[k] = v
                data.append(processed)
            return data
    except FileNotFoundError:
        print(f"Error: Could not find file {csv_file}")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def get_unique_sorted(data, key):
    """Get sorted unique values for a key from the data list of dicts."""
    s = set()
    for row in data:
        s.add(row[key])
    return sorted(s)

def first_for_client(data, key, client):
    """Get the value of dv[key] for the first row matching client count"""
    for row in data:
        if row['clients'] == client:
            return row[key]
    return None

def plot_throughput_vs_clients(data_all, output_file=None, figsize=(35, 15), mixed_workload=False):
    """Plot throughput metrics vs number of clients."""

    # print(data_all)


    zipf_skews = [row["zipf_skew"] for row in data_all[0]]
    read_ratios = [row["read_ratio"] for row in data_all[0]]

    # Plot 1: Transactions per second
    # TODO:
    # Instead instead use successful_transactions and divide by total duration
    # Need to get goodput here, not just total txns.
    # Colorize based on unique zipf_skew values
    # We'll use a color map to assign colors based on the unique zipf_skew values


    unique_zipf_skews = sorted(set(row["zipf_skew"] for row in data_all[0]))
    unique_read_ratios = sorted(set(row["read_ratio"] for row in data_all[0]))

    # unique_zipf_skews = list(filter(lambda x: float(x) == 0.7, unique_zipf_skews))


    fig, axs = plt.subplots(2, len(unique_read_ratios), figsize=figsize)
    # fig.suptitle('RocksDB Benchmark Results: Performance vs Number of Clients', fontsize=16, fontweight='bold')
    clients = get_unique_sorted(data_all[0], 'clients')

    ABORT_MODES = [0,1,2,3]
    # INSERT_YOUR_CODE
    # Choose distinct color for each abort mode using a basic plt colormap ('tab10')
    cmap = plt.get_cmap("tab10")
    colors = [cmap(i) for i in range(len(ABORT_MODES))]
    linestyles = ['-', '--', '-.', '-']
    point_styles = ['o', 's', '^', 'x']

    # print(data_all.keys())
    for col, read_ratio in enumerate(unique_read_ratios):
        # If axs is 1-dimensional, don't index into column.
        if len(axs.shape) == 1:
            ax1 = axs[0]
            # ax2 = axs[1]
            ax3 = axs[1]
        else:
            ax1 = axs[0,col]
            # ax2 = axs[1,col]
            ax3 = axs[1,col]
        for zipfind,zipf_skew in enumerate(unique_zipf_skews):

            # Plot abort modes 1 and 2.
            for color, abort_mode in zip(colors, ABORT_MODES):
                data = data_all[abort_mode]
                my_clients = get_unique_sorted(data, 'clients')
                label = f'zipf_skew={zipf_skew}' if isinstance(zipf_skew, (int, float)) else str(zipf_skew)
                # data_filtered = [row for row in data if float(row["zipf_skew"]) == float(zipf_skew) and float(row["read_ratio"]) == float(read_ratio)]
                data_filtered = [row for row in data if float(row["read_ratio"]) == float(read_ratio)]
                # print(data_filtered)
                tps = [
                    float(first_for_client(data_filtered, 'successful_transactions', c)) / float(first_for_client(data_filtered, 'actual_duration_sec', c))
                    for c in my_clients
                ]
                mean_latency = [
                    float(first_for_client(data_filtered, 'mean_latency_ms', c))
                    for c in my_clients
                ]

                # Plot 1: Transactions per second
                ax1.plot(my_clients, tps, color=color, marker=point_styles[abort_mode], linewidth=3, markersize=16, linestyle=linestyles[abort_mode], label=label)
                # ax1.set_xlabel('Number of Clients')
                # ax1.set_xlabel('Transaction Goodput (txns/sec)')
                if col == 0:
                    ax1.set_ylabel('Goodput (committed txns/sec)', fontsize=22)
                else:
                    ax1.set_ylabel('')
                # ax1.set_ylabel('Mean Latency (ms)')
                if not mixed_workload:
                    ax1.set_title(f'Read ratio={read_ratio}', fontsize=24, fontweight='bold')
                ax1.grid(True, alpha=0.3)
                # INSERT_YOUR_CODE
                # Add abort mode annotations to ax1 (Transactions per second plot)
                if len(my_clients) > 0 and len(tps) > 0:
                    mode_label = {
                        0: "Atomic Snapshot",
                        1: "SIClassic",
                        2: "SIRefined",
                        3: "WSI"
                    }
                    # ax1.annotate(
                    #     f"{mode_label[abort_mode]}",
                    #     (my_clients[-1], tps[-1]),
                    #     textcoords="offset points",
                    #     xytext=(12, 0),
                    #     ha='left',
                    #     fontsize=18,
                    #     color=color,
                    #     fontweight='bold',
                    #     bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=color, alpha=0.7)
                    # )



                # ax2.plot(tps, mean_latency, color=color, marker=point_styles[zipfind], linewidth=2, markersize=8, linestyle=linestyles[zipfind], label=label)
                # # ax2.plot(my_clients, tps, color=color, marker=point_styles[zipfind], linewidth=2, markersize=8, linestyle=linestyles[zipfind], label=label)
                # # ax2.set_xlabel('Number of Clients')
                # ax2.set_xlabel('Transaction Goodput (txns/sec)')
                # # ax2.set_ylabel('Transactions per Second')
                # ax2.set_ylabel('Mean Latency (ms)')
                # ax2.set_title(f'Transaction Throughput (Read Ratio={read_ratio})')
                # ax2.grid(True, alpha=0.3)
                # # INSERT_YOUR_CODE
                # # Annotate the curve with the abort mode tag
                # if len(tps) > 0 and len(mean_latency) > 0:
                #     # Annotate at the last (rightmost) point of the curve for visibility
                #     mode_label = {
                #         0: "No conflicts",
                #         1: "Classic SI",
                #         2: "Refined SI"
                #     }
                #     ax2.annotate(
                #         f"{mode_label[abort_mode]}",
                #         (tps[-1], mean_latency[-1]),
                #         textcoords="offset points",
                #         xytext=(12, 0),
                #         ha='left',
                #         fontsize=8,
                #         color=color,
                #         fontweight='bold',
                #         bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=color, alpha=0.7)
                #     )


            
                
                # for i, c in enumerate(clients):
                #     value = tps[i]
                #     ax1.annotate(f'{value:.0f}', (c, value), textcoords="offset points", 
                #                 xytext=(0,10), ha='center')

                # Plot 3: Success rate
                sr = [100-float(first_for_client(data_filtered, 'success_rate', c)) for c in my_clients]
                ax3.plot(my_clients, sr, color=color, marker=point_styles[abort_mode], linewidth=3, markersize=16, linestyle=linestyles[abort_mode], label='Abort Rate')
                ax3.set_xlabel('Clients', fontsize=22)
                if col == 0:
                    ax3.set_ylabel('Abort Rate (%)', fontsize=22)
                else:
                    ax3.set_ylabel('')
                # ax3.set_title('Transaction Abort Rate')
                ax3.set_ylim(-5, 100)
                ax3.grid(True, alpha=0.3)
                # ax3.legend()
                for i, c in enumerate(my_clients):
                    value = sr[i] 
                    ax3.annotate(f'{value:.1f}%', (c, value), textcoords="offset points", 
                                xytext=(0,10), ha='center', fontsize=8)

        # if col == 0:
            # ax1.legend(["No conflicts", "Classic SI", "Refined SI", "WSI"], fontsize=15)
    # INSERT_YOUR_CODE
    # Move the legend to the top, spanning across all subplots
    # This must be done after plotting, on the main `fig` object, using the "handles" and "labels" from ax1
    handles, labels = ax1.get_legend_handles_labels()
    abort_mode_labels = ["Atomic Snapshot", "SIClassic", "SIRefined", "WSI"]
    fig.legend(
        handles,
        abort_mode_labels,
        loc='upper center',
        bbox_to_anchor=(0.5, 1.06),
        ncol=len(abort_mode_labels),
        fontsize=24,
        frameon=False
    )

    # # Plot 2: Operations per second
    # ops = [first_for_client(data, 'operations_per_sec', c) for c in clients]
    # ax2.plot(clients, ops, 'ro-', linewidth=2, markersize=8, label='Operations/sec')
    # ax2.set_xlabel('Number of Clients')
    # ax2.set_ylabel('Operations per Second')
    # ax2.set_title('Operation Throughput')
    # ax2.grid(True, alpha=0.3)
    # ax2.legend()
    # for i, c in enumerate(clients):
    #     value = ops[i]
    #     ax2.annotate(f'{value:.0f}', (c, value), textcoords="offset points", 
    #                 xytext=(0,10), ha='center')
    

    # # Plot 4: Mean latency
    # ml = [first_for_client(data, 'mean_latency_ms', c) for c in clients]
    # ax4.set_xlabel('Number of Clients')
    # ax4.set_ylabel('Mean Latency (ms)')
    # ax4.set_title('Transaction Latency')
    # ax4.grid(True, alpha=0.3)
    # mean latency plot is commented in original
    # # ax4.plot(clients, ml, 'mo-', linewidth=2, markersize=8, label="Mean Latency")
    # ax4.legend()
    # for i, c in enumerate(clients):
    #     value = ml[i]
    #     ax4.annotate(f'{value:.1f}ms', (c, value), textcoords="offset points", 
    #                 xytext=(0,10), ha='center')

    plt.tight_layout()
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_file}")
    plt.show()

def plot_scalability_analysis(data, output_file=None):
    """Create a scalability analysis plot showing efficiency."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle('RocksDB Scalability Analysis', fontsize=16, fontweight='bold')
    clients = get_unique_sorted(data, 'clients')

    # Calculate throughput per client (efficiency)
    throughput_per_client = []
    tps_total = []
    for c in clients:
        tps = first_for_client(data, 'transactions_per_sec', c)
        tps_total.append(tps)
        throughput_per_client.append(tps / c if c != 0 else 0)

    # Plot 1: Throughput per client (efficiency)
    ax1.plot(clients, throughput_per_client, 'bo-', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Clients')
    ax1.set_ylabel('Transactions per Second per Client')
    ax1.set_title('Throughput Efficiency (per Client)')
    ax1.grid(True, alpha=0.3)
    for i, c in enumerate(clients):
        value = throughput_per_client[i]
        ax1.annotate(f'{value:.0f}', (c, value), textcoords="offset points", 
                    xytext=(0,10), ha='center')

    # Plot 2: Total throughput vs clients (scalability)
    ax2.plot(clients, tps_total, 'ro-', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Clients')
    ax2.set_ylabel('Total Transactions per Second')
    ax2.set_title('Total Throughput Scalability')
    ax2.grid(True, alpha=0.3)
    for i, c in enumerate(clients):
        value = tps_total[i]
        ax2.annotate(f'{value:.0f}', (c, value), textcoords="offset points", 
                    xytext=(0,10), ha='center')

    plt.tight_layout()
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Scalability plot saved to {output_file}")
    plt.show()

def print_summary_stats(data):
    """Print summary statistics from the benchmark data."""
    print("\n=== Benchmark Summary ===")
    print(f"Total benchmark runs: {len(data)}")
    clients = get_unique_sorted(data, "clients")
    print(f"Client configurations: {clients}")
    if data and len(data) > 0:
        print(f"Read ratio: {data[0].get('read_ratio',0):.1%}")
        print(f"Keys per transaction: {data[0].get('keys_per_txn',0)}")
        print(f"Keyspace size: {data[0].get('keyspace_size',0)}")
    print("\n=== Performance Summary ===")
    for row in data:
        print(f"Clients: {row['clients']:2d} | "
              f"TPS: {row['transactions_per_sec']:8.1f} | "
              f"OPS: {row['operations_per_sec']:8.1f} | "
              f"Success: {row['success_rate']:5.1f}% | "
            #   f"Latency: {row['mean_latency_ms']:6.2f}ms"
              )

def main():
    parser = argparse.ArgumentParser(description='Plot RocksDB benchmark results')
    parser.add_argument('--csv', default='benchmark_results.csv', 
                       help='Path to benchmark results CSV file')
    parser.add_argument('--output', help='Output file for the main plot')
    # parser.add_argument('--scalability-output', help='Output file for scalability plot')
    parser.add_argument('--no-show', action='store_true', 
                       help='Don\'t display plots (useful for headless environments)')
    args = parser.parse_args()

    data = {}
    mixed_data = {}
    for abort_mode in [0,1,2,3]:
        benchmark_csv_file = f"benchmark_results_{abort_mode}.csv"
        print(f"Loading data from {benchmark_csv_file}")
        data[abort_mode] = load_benchmark_data(benchmark_csv_file)
        if data[abort_mode] is None:
            return 1

       # TODO: Mixed workload data.
        mixed_benchmark_csv_file = f"benchmark_results_{abort_mode}_mixed.csv"
        print(f"Loading data from {benchmark_csv_file}")
        mixed_data[abort_mode] = load_benchmark_data(mixed_benchmark_csv_file)
        if mixed_data[abort_mode] is None:
            return 1

    plot_throughput_vs_clients(data, args.output, figsize=(35, 13))
    plot_throughput_vs_clients(mixed_data, args.output.replace(".png", "_mixed.png"), figsize=(12, 13), mixed_workload=True)

    return 0

if __name__ == '__main__':
    exit(main())
