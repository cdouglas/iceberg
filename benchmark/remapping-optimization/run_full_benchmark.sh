#!/bin/bash
#
# Full Remapping Algorithm Benchmark Suite
#
# This script runs the complete JMH benchmark suite, performs analysis,
# generates visualizations, and creates a summary report.
#
# Usage: ./run_full_benchmark.sh [--quick]
#   --quick: Run abbreviated benchmarks (fewer iterations)
#
# Duration: ~2-3 hours for full suite, ~30 minutes for quick mode
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# JMH writes to default location - we'll copy from there
JMH_OUTPUT_DIR="$PROJECT_ROOT/core/build/reports/jmh"

# Output files in benchmark directory
RESULTS_TXT="$SCRIPT_DIR/results_${TIMESTAMP}.txt"
RESULTS_JSON="$SCRIPT_DIR/results_${TIMESTAMP}.json"
RESULTS_CSV="$SCRIPT_DIR/results_${TIMESTAMP}.csv"
REPORT_FILE="$SCRIPT_DIR/BENCHMARK_REPORT_${TIMESTAMP}.md"

# Check for quick mode
QUICK_MODE=false
JMH_ARGS=""
if [[ "$1" == "--quick" ]]; then
    QUICK_MODE=true
    JMH_ARGS="-PjmhArgs=-wi 1 -i 2 -f 1"
    echo "Running in QUICK MODE (abbreviated benchmarks)"
fi

echo "=============================================="
echo "Remapping Algorithm Benchmark Suite"
echo "=============================================="
echo "Timestamp: $TIMESTAMP"
echo "Output directory: $SCRIPT_DIR"
echo ""

# Step 1: Run JMH Benchmarks
echo "[1/4] Running JMH Benchmarks..."
echo "      This may take 2-3 hours for the full suite."
echo ""

cd "$PROJECT_ROOT"

# Run JMH - results go to default location (core/build/reports/jmh/)
if $QUICK_MODE; then
    ./gradlew :iceberg-core:jmh \
        -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
        $JMH_ARGS \
        --no-daemon
else
    ./gradlew :iceberg-core:jmh \
        -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
        --no-daemon
fi

# Copy results from JMH default location to benchmark directory
echo ""
echo "Copying results from JMH output directory..."

if [[ -f "$JMH_OUTPUT_DIR/human-readable-output.txt" ]]; then
    cp "$JMH_OUTPUT_DIR/human-readable-output.txt" "$RESULTS_TXT"
    echo "  Copied: $RESULTS_TXT"
else
    echo "  ERROR: JMH text output not found at $JMH_OUTPUT_DIR/human-readable-output.txt"
    exit 1
fi

if [[ -f "$JMH_OUTPUT_DIR/results.json" ]]; then
    cp "$JMH_OUTPUT_DIR/results.json" "$RESULTS_JSON"
    echo "  Copied: $RESULTS_JSON"
else
    echo "  WARNING: JMH JSON output not found"
fi

echo ""
echo "[1/4] Benchmarks complete."
echo "      Results: $RESULTS_TXT"
echo "      JSON:    $RESULTS_JSON"
echo ""

# Step 2: Run Analysis
echo "[2/4] Running Analysis..."

cd "$SCRIPT_DIR"

# Activate virtual environment if it exists
if [[ -d "venv" ]]; then
    source venv/bin/activate
fi

python3 analyze_results.py "$RESULTS_TXT"

# The analysis script generates a CSV with the same base name
if [[ -f "${RESULTS_TXT%.txt}.csv" ]]; then
    RESULTS_CSV="${RESULTS_TXT%.txt}.csv"
    echo "  CSV generated: $RESULTS_CSV"
fi

echo ""
echo "[2/4] Analysis complete."
echo ""

# Step 3: Generate Visualizations
echo "[3/4] Generating Visualizations..."

if [[ -f "$RESULTS_CSV" ]]; then
    python3 visualize_results.py "$RESULTS_CSV"
    echo "      Charts generated in $SCRIPT_DIR/"
else
    echo "      WARNING: CSV file not found, skipping visualization"
fi

echo ""
echo "[3/4] Visualizations complete."
echo ""

# Step 4: Generate Report
echo "[4/4] Generating Benchmark Report..."

cat > "$REPORT_FILE" << 'REPORT_HEADER'
# Remapping Algorithm Benchmark Report

REPORT_HEADER

echo "**Generated:** $(date '+%Y-%m-%d %H:%M:%S')" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "**Benchmark Timestamp:** $TIMESTAMP" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

if $QUICK_MODE; then
    echo "**Mode:** Quick (abbreviated iterations)" >> "$REPORT_FILE"
else
    echo "**Mode:** Full benchmark suite" >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << 'REPORT_OVERVIEW'

## Overview

This report contains performance benchmarks for the remapping algorithm strategies
used in compaction map position delete remapping.

### Strategies Benchmarked

| Strategy | Complexity | Best For |
|----------|------------|----------|
| LinearSearch | O(n × m) | Baseline comparison |
| BinarySearch | O(n × log m) | General purpose |
| IntervalTree | O(n × log m) | Unsorted positions |
| StreamJoin | O(n + m) | Sorted positions, large m |
| RangeQuery | O(m × log n) | Few runs (small m) |
| SmartSelector | Varies | Automatic optimal selection |

### Parameters

- **numRuns (m):** 10, 100, 1000 - Number of runs in compaction map
- **numPositions (n):** 1000, 10000, 100000 - Number of positions to remap
- **gapRatio:** 0.0 (dense), 0.3 (moderate), 0.5 (sparse)
- **sorted:** true/false - Whether input positions are sorted

## Results Summary

REPORT_OVERVIEW

# Extract benchmark results table from the text output
if [[ -f "$RESULTS_TXT" ]]; then
    echo '```' >> "$REPORT_FILE"
    # Extract the final results table (starts with "Benchmark" header line)
    grep -A 500 "^Benchmark.*Mode.*Cnt.*Score" "$RESULTS_TXT" | head -350 >> "$REPORT_FILE" 2>/dev/null || echo "Results table not found" >> "$REPORT_FILE"
    echo '```' >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << 'REPORT_CHARTS'

## Visualizations

### Strategy Comparison
![Strategy Comparison](chart_strategy_comparison.png)

### Smart Selector Overhead
![Selector Overhead](chart_selector_overhead.png)

### Speedup vs Linear Search
![Speedup vs Linear](chart_speedup_vs_linear.png)

## Key Findings

### Smart Selector Performance

The smart selector automatically chooses the optimal strategy based on:
- Number of runs (m)
- Number of positions (n)
- Whether positions are sorted
- Gap ratio (sparsity)

Expected overhead: **< 10%** compared to manually selecting the optimal strategy.

### Strategy Selection Rules

1. **Few runs (m < 10):**
   - Sorted → RangeQuery
   - Unsorted → BinarySearch

2. **High fan-in (n/m > 100) with gaps:**
   - Sorted → RangeQuery
   - Unsorted → IntervalTree

3. **Medium runs (m < 100):**
   - Sorted with n > m → StreamJoin
   - Otherwise → BinarySearch

4. **Many runs (m ≥ 100):**
   - IntervalTree (always optimal)

## Files Generated

REPORT_CHARTS

echo "- \`results_${TIMESTAMP}.txt\` - Full benchmark output" >> "$REPORT_FILE"
echo "- \`results_${TIMESTAMP}.json\` - JSON results for programmatic analysis" >> "$REPORT_FILE"
echo "- \`results_${TIMESTAMP}.csv\` - CSV for spreadsheet analysis" >> "$REPORT_FILE"
echo "- \`chart_strategy_comparison.png\` - Strategy performance comparison" >> "$REPORT_FILE"
echo "- \`chart_selector_overhead.png\` - Smart selector overhead analysis" >> "$REPORT_FILE"
echo "- \`chart_speedup_vs_linear.png\` - Speedup vs baseline" >> "$REPORT_FILE"

echo "" >> "$REPORT_FILE"
echo "---" >> "$REPORT_FILE"
echo "*Report generated by run_full_benchmark.sh*" >> "$REPORT_FILE"

echo ""
echo "[4/4] Report complete: $REPORT_FILE"
echo ""

# Deactivate virtual environment if we activated it
if [[ -d "venv" ]]; then
    deactivate 2>/dev/null || true
fi

echo "=============================================="
echo "Benchmark Suite Complete"
echo "=============================================="
echo ""
echo "Output files:"
echo "  - $RESULTS_TXT"
echo "  - $RESULTS_JSON"
echo "  - $RESULTS_CSV"
echo "  - $REPORT_FILE"
echo "  - chart_strategy_comparison.png"
echo "  - chart_selector_overhead.png"
echo "  - chart_speedup_vs_linear.png"
echo ""
echo "To view the report:"
echo "  cat $REPORT_FILE"
echo ""
