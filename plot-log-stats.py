#!/usr/bin/env python3
import json
import os
import glob
from datetime import datetime
from collections import defaultdict
import argparse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import humanize

def parse_args():
    parser = argparse.ArgumentParser(description='Generate size plots from git-annex JSON statistics files')
    parser.add_argument('input_pattern', help='Glob pattern for input JSON files (e.g., "stats/*.json")')
    parser.add_argument('--output', '-o', default='size_history.png', help='Output plot filename')
    parser.add_argument('--title', '-t', default='Git and Git-Annex Size History', help='Plot title')
    parser.add_argument('--log-scale', '-l', action='store_true', help='Use logarithmic scale for size axis')
    return parser.parse_args()

def load_json_files(pattern):
    """Load all JSON files matching the pattern."""
    all_data = []
    for filename in glob.glob(pattern, recursive=True):
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                all_data.append(data)
            print(f"Loaded {filename} with {len(data)} entries")
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    return all_data

def get_month_range(all_data):
    """Get the range of months from the earliest to the latest commit."""
    min_date = None
    max_date = None
    
    for repo_data in all_data:
        for commit_hash, commit_data in repo_data.items():
            try:
                timestamp = datetime.fromisoformat(commit_data['timestamp'])
                
                if min_date is None or timestamp < min_date:
                    min_date = timestamp
                
                if max_date is None or timestamp > max_date:
                    max_date = timestamp
            except Exception:
                continue
    
    if min_date is None or max_date is None:
        return []
    
    # Generate list of all months in the range
    months = []
    current = datetime(min_date.year, min_date.month, 1)
    end = datetime(max_date.year, max_date.month, 1)
    
    while current <= end:
        months.append(current.strftime('%Y-%m'))
        # Move to next month
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    return months

def aggregate_by_month(all_data):
    """Aggregate data by month, carrying forward values for missing months."""
    # Get the full range of months
    all_months = get_month_range(all_data)
    if not all_months:
        return {}
    
    # Initialize monthly totals
    monthly_totals = {month: {'git_size': 0, 'annex_size': 0, 'total_size': 0} for month in all_months}
    
    # Process each repository
    for repo_data in all_data:
        # Convert repository data to chronological list
        repo_commits = []
        for commit_hash, commit_data in repo_data.items():
            try:
                timestamp = datetime.fromisoformat(commit_data['timestamp'])
                repo_commits.append({
                    'month': timestamp.strftime('%Y-%m'),
                    'git_size': commit_data['git_size'],
                    'annex_size': commit_data['annex_size'],
                    'total_size': commit_data['total_size']
                })
            except Exception as e:
                print(f"Error processing commit {commit_hash}: {e}")
        
        # Sort by month
        repo_commits.sort(key=lambda x: x['month'])
        
        # Find the largest total size for each month in this repo
        repo_monthly = {}
        for commit in repo_commits:
            month = commit['month']
            if month not in repo_monthly or commit['total_size'] > repo_monthly[month]['total_size']:
                repo_monthly[month] = commit
        
        # Carry forward values for missing months
        last_values = {'git_size': 0, 'annex_size': 0, 'total_size': 0}
        for month in all_months:
            if month in repo_monthly:
                # Update with this month's values
                last_values = {
                    'git_size': repo_monthly[month]['git_size'],
                    'annex_size': repo_monthly[month]['annex_size'],
                    'total_size': repo_monthly[month]['total_size']
                }
            
            # Add to monthly totals (either new values or carried forward)
            monthly_totals[month]['git_size'] += last_values['git_size']
            monthly_totals[month]['annex_size'] += last_values['annex_size']
            monthly_totals[month]['total_size'] += last_values['total_size']
    
    return monthly_totals

def create_plot(monthly_data, output_filename, title, use_log_scale):
    """Create a plot of the monthly data with humanized size labels."""
    # Sort months chronologically
    sorted_months = sorted(monthly_data.keys())
    dates = [datetime.strptime(month, '%Y-%m') for month in sorted_months]
    
    # Extract size data
    git_sizes = [monthly_data[month]['git_size'] for month in sorted_months]
    annex_sizes = [monthly_data[month]['annex_size'] for month in sorted_months]
    total_sizes = [monthly_data[month]['total_size'] for month in sorted_months]
    
    # Create plot
    plt.figure(figsize=(12, 8))
    
    plt.plot(dates, git_sizes, 'b:', label='Git Objects', linewidth=2)
    plt.plot(dates, annex_sizes, 'g--', label='Git-Annex Files', linewidth=2)
    plt.plot(dates, total_sizes, 'r-', label='Total Size', linewidth=2)
    
    # Format the plot
    plt.title(title, fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Size', fontsize=14)
    
    # Use log scale if requested
    if use_log_scale and max(total_sizes) > 0:
        plt.yscale('log')
    
    # Format the x-axis to show dates nicely
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45)
    
    # Format y-axis with humanized sizes
    def size_formatter(x, pos):
        return humanize.naturalsize(x, binary=True)
    
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(size_formatter))
    
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(fontsize=12)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_filename, dpi=300)
    print(f"Plot saved to {output_filename}")
    
    # Also display it if running in an interactive environment
    plt.show()

def main():
    args = parse_args()
    
    # Load all JSON files
    all_data = load_json_files(args.input_pattern)
    if not all_data:
        print("No data found. Check the input pattern.")
        return
    
    # Aggregate data by month
    monthly_data = aggregate_by_month(all_data)
    if not monthly_data:
        print("No valid data found in the JSON files.")
        return
    
    # Create the plot
    create_plot(monthly_data, args.output, args.title, args.log_scale)

if __name__ == '__main__':
    main()