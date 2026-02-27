#!/bin/bash

# abort_mode=$1

# Install Python dependencies if needed
# if ! python3 -c "import pandas, matplotlib, numpy" 2>/dev/null; then
#     echo "Installing Python dependencies..."
#     pip3 install -r requirements.txt
# fi

# Run the plotting script
echo "Generating plots from benchmark_results.csv..."
python3 plot_benchmark.py --csv benchmark_results.csv --output benchmark_throughput.png

echo "Plots generated."
