#!/usr/bin/env python3
"""
Benchmark Results Analyzer for Distributed File System
Extracts and compares performance metrics from .ansi benchmark files
across different cache strategies (LFU, LRU, No Cache) and benchmark types.
"""

import os
import re
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from tabulate import tabulate


@dataclass
class BenchmarkResult:
    """Represents parsed benchmark results from an .ansi file."""
    cache_type: str
    benchmark_type: str
    total_operations: int = 0
    successful_ops: int = 0
    failed_ops: int = 0
    total_bytes: int = 0
    total_bytes_mb: float = 0.0
    total_time_seconds: float = 0.0
    throughput_mb_s: float = 0.0
    ops_per_sec: int = 0
    avg_latency_ms: float = 0.0
    min_latency_ms: Optional[float] = None
    p50_latency_ms: Optional[float] = None
    p99_latency_ms: Optional[float] = None
    max_latency_ms: Optional[float] = None
    phase_throughputs: List[float] = field(default_factory=list)


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI escape codes from text."""
    ansi_pattern = re.compile(r'\x1b\[[0-9;]*m|\[([0-9;]*)m')
    return ansi_pattern.sub('', text)


def parse_ansi_file(filepath: Path, cache_type: str, benchmark_type: str) -> BenchmarkResult:
    """Parse an .ansi benchmark file and extract performance metrics."""
    result = BenchmarkResult(cache_type=cache_type, benchmark_type=benchmark_type)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Strip ANSI codes for easier parsing
    content = strip_ansi_codes(content)
    
    # Extract Total Operations
    match = re.search(r'Total Operations:\s*(\d+)', content)
    if match:
        result.total_operations = int(match.group(1))
    
    # Extract Successful Ops
    match = re.search(r'Successful Ops:\s*(\d+)', content)
    if match:
        result.successful_ops = int(match.group(1))
    
    # Extract Failed Ops
    match = re.search(r'Failed Ops:\s*(\d+)', content)
    if match:
        result.failed_ops = int(match.group(1))
    
    # Extract Total Bytes
    match = re.search(r'Total Bytes:\s*(\d+)\s*\(([\d.]+)\s*MB\)', content)
    if match:
        result.total_bytes = int(match.group(1))
        result.total_bytes_mb = float(match.group(2))
    
    # Extract Total Time
    match = re.search(r'Total Time:\s*([\d.]+)\s*seconds', content)
    if match:
        result.total_time_seconds = float(match.group(1))
    
    # Extract Throughput
    match = re.search(r'Throughput:\s*([\d.]+)\s*MB/s', content)
    if match:
        result.throughput_mb_s = float(match.group(1))
    
    # Extract Ops/sec
    match = re.search(r'Ops/sec:\s*(\d+)', content)
    if match:
        result.ops_per_sec = int(match.group(1))
    
    # Extract Latency Statistics
    match = re.search(r'Avg Latency:\s*([\d.]+)\s*ms', content)
    if match:
        result.avg_latency_ms = float(match.group(1))
    
    match = re.search(r'Min Latency:\s*([\d.]+)\s*ms', content)
    if match:
        result.min_latency_ms = float(match.group(1))
    
    match = re.search(r'P50 Latency:\s*([\d.]+)\s*ms', content)
    if match:
        result.p50_latency_ms = float(match.group(1))
    
    match = re.search(r'P99 Latency:\s*([\d.]+)\s*ms', content)
    if match:
        result.p99_latency_ms = float(match.group(1))
    
    match = re.search(r'Max Latency:\s*([\d.]+)\s*ms', content)
    if match:
        result.max_latency_ms = float(match.group(1))
    
    # Extract per-phase throughputs (for random read and sequential read)
    phase_matches = re.findall(r'(?:Phase|Iteration)\s*\d+(?:/\d+)?[:\s]*([\d.]+)\s*MB/s', content)
    if phase_matches:
        result.phase_throughputs = [float(t) for t in phase_matches]
    
    return result


def collect_all_results(base_path: Path) -> Dict[str, Dict[str, BenchmarkResult]]:
    """
    Collect all benchmark results from the v1 directory.
    Returns a nested dict: {benchmark_type: {cache_type: BenchmarkResult}}
    """
    results: Dict[str, Dict[str, BenchmarkResult]] = {}
    
    cache_types = ['lfu', 'lru', 'no_cache']
    benchmark_types = ['random_read', 'random_write', 'sequential_read', 'sequential_write']
    
    for cache_type in cache_types:
        cache_dir = base_path / cache_type
        if not cache_dir.exists():
            print(f"Warning: Directory {cache_dir} does not exist")
            continue
        
        for benchmark_type in benchmark_types:
            ansi_file = cache_dir / f"{benchmark_type}.ansi"
            if not ansi_file.exists():
                print(f"Warning: File {ansi_file} does not exist")
                continue
            
            result = parse_ansi_file(ansi_file, cache_type, benchmark_type)
            
            if benchmark_type not in results:
                results[benchmark_type] = {}
            results[benchmark_type][cache_type] = result
    
    return results


def format_benchmark_name(name: str) -> str:
    """Format benchmark type name for display."""
    return name.replace('_', ' ').title()


def format_cache_name(name: str) -> str:
    """Format cache type name for display."""
    if name == 'no_cache':
        return 'No Cache'
    return name.upper()


def calculate_improvement(value: float, baseline: float) -> str:
    """Calculate percentage improvement over baseline."""
    if baseline == 0:
        return "N/A"
    improvement = ((value - baseline) / baseline) * 100
    if improvement > 0:
        return f"+{improvement:.1f}%"
    return f"{improvement:.1f}%"


def print_comparison_table(results: Dict[str, Dict[str, BenchmarkResult]]) -> None:
    """Print comprehensive comparison tables for all benchmarks."""
    
    print("\n" + "=" * 80)
    print("DISTRIBUTED FILE SYSTEM - BENCHMARK PERFORMANCE ANALYSIS")
    print("=" * 80)
    
    # Order of cache types for display (no_cache as baseline)
    cache_order = ['no_cache', 'lru', 'lfu']
    benchmark_order = ['sequential_read', 'sequential_write', 'random_read', 'random_write']
    
    # =====================================================================
    # THROUGHPUT COMPARISON TABLE
    # =====================================================================
    print("\n" + "-" * 80)
    print("THROUGHPUT COMPARISON (MB/s)")
    print("-" * 80)
    
    headers = ['Benchmark Type'] + [format_cache_name(c) for c in cache_order] + ['LRU vs No Cache', 'LFU vs No Cache', 'Best']
    table_data = []
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        row = [format_benchmark_name(benchmark_type)]
        throughputs = {}
        
        for cache_type in cache_order:
            if cache_type in results[benchmark_type]:
                throughput = results[benchmark_type][cache_type].throughput_mb_s
                throughputs[cache_type] = throughput
                row.append(f"{throughput:.2f}")
            else:
                row.append("N/A")
                throughputs[cache_type] = 0
        
        # Calculate improvements vs no_cache baseline
        baseline = throughputs.get('no_cache', 0)
        lru_improvement = calculate_improvement(throughputs.get('lru', 0), baseline)
        lfu_improvement = calculate_improvement(throughputs.get('lfu', 0), baseline)
        row.append(lru_improvement)
        row.append(lfu_improvement)
        
        # Determine best performer
        best_cache = max(throughputs, key=throughputs.get) if throughputs else "N/A"
        row.append(format_cache_name(best_cache))
        
        table_data.append(row)
    
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # =====================================================================
    # LATENCY COMPARISON TABLE
    # =====================================================================
    print("\n" + "-" * 80)
    print("AVERAGE LATENCY COMPARISON (ms) - Lower is Better")
    print("-" * 80)
    
    headers = ['Benchmark Type'] + [format_cache_name(c) for c in cache_order] + ['LRU vs No Cache', 'LFU vs No Cache', 'Best']
    table_data = []
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        row = [format_benchmark_name(benchmark_type)]
        latencies = {}
        
        for cache_type in cache_order:
            if cache_type in results[benchmark_type]:
                latency = results[benchmark_type][cache_type].avg_latency_ms
                latencies[cache_type] = latency
                row.append(f"{latency:.2f}")
            else:
                row.append("N/A")
                latencies[cache_type] = float('inf')
        
        # Calculate improvements vs no_cache baseline (negative is better for latency)
        baseline = latencies.get('no_cache', 0)
        if baseline > 0:
            lru_lat = latencies.get('lru', baseline)
            lfu_lat = latencies.get('lfu', baseline)
            lru_improvement = f"{((baseline - lru_lat) / baseline) * 100:.1f}%" if lru_lat < float('inf') else "N/A"
            lfu_improvement = f"{((baseline - lfu_lat) / baseline) * 100:.1f}%" if lfu_lat < float('inf') else "N/A"
        else:
            lru_improvement = "N/A"
            lfu_improvement = "N/A"
        
        row.append(lru_improvement)
        row.append(lfu_improvement)
        
        # Determine best performer (lowest latency)
        valid_latencies = {k: v for k, v in latencies.items() if v < float('inf')}
        best_cache = min(valid_latencies, key=valid_latencies.get) if valid_latencies else "N/A"
        row.append(format_cache_name(best_cache))
        
        table_data.append(row)
    
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # =====================================================================
    # OPERATIONS PER SECOND TABLE
    # =====================================================================
    print("\n" + "-" * 80)
    print("OPERATIONS PER SECOND COMPARISON")
    print("-" * 80)
    
    headers = ['Benchmark Type'] + [format_cache_name(c) for c in cache_order] + ['LRU vs No Cache', 'LFU vs No Cache']
    table_data = []
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        row = [format_benchmark_name(benchmark_type)]
        ops = {}
        
        for cache_type in cache_order:
            if cache_type in results[benchmark_type]:
                ops_sec = results[benchmark_type][cache_type].ops_per_sec
                ops[cache_type] = ops_sec
                row.append(f"{ops_sec:,}")
            else:
                row.append("N/A")
                ops[cache_type] = 0
        
        # Calculate improvements
        baseline = ops.get('no_cache', 0)
        row.append(calculate_improvement(ops.get('lru', 0), baseline))
        row.append(calculate_improvement(ops.get('lfu', 0), baseline))
        
        table_data.append(row)
    
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # =====================================================================
    # P99 LATENCY TABLE
    # =====================================================================
    print("\n" + "-" * 80)
    print("P99 LATENCY COMPARISON (ms) - Lower is Better")
    print("-" * 80)
    
    headers = ['Benchmark Type'] + [format_cache_name(c) for c in cache_order]
    table_data = []
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        row = [format_benchmark_name(benchmark_type)]
        
        for cache_type in cache_order:
            if cache_type in results[benchmark_type]:
                p99 = results[benchmark_type][cache_type].p99_latency_ms
                if p99 is not None:
                    row.append(f"{p99:.2f}")
                else:
                    row.append("N/A")
            else:
                row.append("N/A")
        
        table_data.append(row)
    
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # =====================================================================
    # DETAILED RESULTS PER BENCHMARK
    # =====================================================================
    print("\n" + "=" * 80)
    print("DETAILED RESULTS BY BENCHMARK TYPE")
    print("=" * 80)
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        print(f"\n{'─' * 80}")
        print(f"  {format_benchmark_name(benchmark_type)}")
        print(f"{'─' * 80}")
        
        headers = ['Metric', 'No Cache', 'LRU', 'LFU']
        table_data = []
        
        # Get results for each cache type
        nc = results[benchmark_type].get('no_cache')
        lru = results[benchmark_type].get('lru')
        lfu = results[benchmark_type].get('lfu')
        
        def safe_get(result, attr, fmt="{:.2f}"):
            if result is None:
                return "N/A"
            val = getattr(result, attr, None)
            if val is None:
                return "N/A"
            if isinstance(val, float):
                return fmt.format(val)
            return str(val)
        
        table_data.append(['Total Operations', 
                          safe_get(nc, 'total_operations', "{}"),
                          safe_get(lru, 'total_operations', "{}"),
                          safe_get(lfu, 'total_operations', "{}")])
        
        table_data.append(['Successful Ops',
                          safe_get(nc, 'successful_ops', "{}"),
                          safe_get(lru, 'successful_ops', "{}"),
                          safe_get(lfu, 'successful_ops', "{}")])
        
        table_data.append(['Total Data (MB)',
                          safe_get(nc, 'total_bytes_mb'),
                          safe_get(lru, 'total_bytes_mb'),
                          safe_get(lfu, 'total_bytes_mb')])
        
        table_data.append(['Total Time (s)',
                          safe_get(nc, 'total_time_seconds'),
                          safe_get(lru, 'total_time_seconds'),
                          safe_get(lfu, 'total_time_seconds')])
        
        table_data.append(['Throughput (MB/s)',
                          safe_get(nc, 'throughput_mb_s'),
                          safe_get(lru, 'throughput_mb_s'),
                          safe_get(lfu, 'throughput_mb_s')])
        
        table_data.append(['Ops/sec',
                          safe_get(nc, 'ops_per_sec', "{}"),
                          safe_get(lru, 'ops_per_sec', "{}"),
                          safe_get(lfu, 'ops_per_sec', "{}")])
        
        table_data.append(['Avg Latency (ms)',
                          safe_get(nc, 'avg_latency_ms'),
                          safe_get(lru, 'avg_latency_ms'),
                          safe_get(lfu, 'avg_latency_ms')])
        
        table_data.append(['P50 Latency (ms)',
                          safe_get(nc, 'p50_latency_ms'),
                          safe_get(lru, 'p50_latency_ms'),
                          safe_get(lfu, 'p50_latency_ms')])
        
        table_data.append(['P99 Latency (ms)',
                          safe_get(nc, 'p99_latency_ms'),
                          safe_get(lru, 'p99_latency_ms'),
                          safe_get(lfu, 'p99_latency_ms')])
        
        print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # =====================================================================
    # SUMMARY AND RECOMMENDATIONS
    # =====================================================================
    print("\n" + "=" * 80)
    print("SUMMARY & ANALYSIS")
    print("=" * 80)
    
    print("\n📊 Performance Rankings by Throughput:\n")
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        throughputs = []
        for cache_type in cache_order:
            if cache_type in results[benchmark_type]:
                throughputs.append((cache_type, results[benchmark_type][cache_type].throughput_mb_s))
        
        throughputs.sort(key=lambda x: x[1], reverse=True)
        
        print(f"  {format_benchmark_name(benchmark_type)}:")
        for i, (cache, throughput) in enumerate(throughputs, 1):
            baseline = results[benchmark_type].get('no_cache')
            if baseline and cache != 'no_cache':
                speedup = throughput / baseline.throughput_mb_s if baseline.throughput_mb_s > 0 else 0
                print(f"    {i}. {format_cache_name(cache)}: {throughput:.2f} MB/s ({speedup:.2f}x vs No Cache)")
            else:
                print(f"    {i}. {format_cache_name(cache)}: {throughput:.2f} MB/s (baseline)")
        print()
    
    # Calculate overall winners
    print("\n🏆 Best Cache Strategy by Benchmark Type:\n")
    
    for benchmark_type in benchmark_order:
        if benchmark_type not in results:
            continue
        
        throughputs = {ct: results[benchmark_type][ct].throughput_mb_s 
                      for ct in cache_order if ct in results[benchmark_type]}
        
        if throughputs:
            best = max(throughputs, key=throughputs.get)
            worst = min(throughputs, key=throughputs.get)
            improvement = ((throughputs[best] / throughputs[worst]) - 1) * 100 if throughputs[worst] > 0 else 0
            print(f"  • {format_benchmark_name(benchmark_type)}: {format_cache_name(best)} "
                  f"(+{improvement:.1f}% faster than {format_cache_name(worst)})")
    
    print("\n" + "=" * 80)


def export_csv(results: Dict[str, Dict[str, BenchmarkResult]], output_path: Path) -> None:
    """Export results to a CSV file for further analysis."""
    import csv
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            'Benchmark Type', 'Cache Type', 'Total Operations', 'Successful Ops',
            'Failed Ops', 'Total Bytes (MB)', 'Total Time (s)', 'Throughput (MB/s)',
            'Ops/sec', 'Avg Latency (ms)', 'Min Latency (ms)', 'P50 Latency (ms)',
            'P99 Latency (ms)', 'Max Latency (ms)'
        ])
        
        # Write data
        for benchmark_type, cache_results in results.items():
            for cache_type, result in cache_results.items():
                writer.writerow([
                    format_benchmark_name(benchmark_type),
                    format_cache_name(cache_type),
                    result.total_operations,
                    result.successful_ops,
                    result.failed_ops,
                    result.total_bytes_mb,
                    result.total_time_seconds,
                    result.throughput_mb_s,
                    result.ops_per_sec,
                    result.avg_latency_ms,
                    result.min_latency_ms or '',
                    result.p50_latency_ms or '',
                    result.p99_latency_ms or '',
                    result.max_latency_ms or ''
                ])
    
    print(f"\n📁 Results exported to: {output_path}")


def main():
    """Main entry point for the benchmark analyzer."""
    # Define the base path for benchmark results
    script_dir = Path(__file__).parent
    base_path = script_dir / "testing_results" / "v1"
    
    if not base_path.exists():
        print(f"Error: Benchmark results directory not found: {base_path}")
        print("Please ensure the testing_results/v1 directory exists with .ansi files.")
        return 1
    
    print(f"📂 Analyzing benchmark results from: {base_path}")
    
    # Collect all results
    results = collect_all_results(base_path)
    
    if not results:
        print("Error: No benchmark results found!")
        return 1
    
    # Print comparison tables
    print_comparison_table(results)
    
    # Export to CSV
    csv_path = script_dir / "benchmark_analysis.csv"
    export_csv(results, csv_path)
    
    return 0


if __name__ == "__main__":
    exit(main())
