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
import itertools

def parse_args():
    parser = argparse.ArgumentParser(description='Generate size plots from git-annex JSON statistics files')
    # Group arguments
    parser.add_argument('--group', action='append', nargs='+', metavar=('NAME', 'PATTERN'),
                        help='Define a group with name and one or more glob patterns')
    # If no groups are defined, use a single pattern
    parser.add_argument('input_pattern', nargs='?', help='Glob pattern for input JSON files (if no groups defined)')
    # Output and formatting options
    parser.add_argument('--output', '-o', default='size_history.png', help='Output plot filename')
    parser.add_argument('--title', '-t', default='Git and Git-Annex Size History', help='Plot title')
    parser.add_argument('--log-scale', '-l', action='store_true', help='Use logarithmic scale for size axis')
    parser.add_argument('--separate-components', '-s', action='store_true',
                        help='Show git and git-annex sizes separately (default: total only)')
    parser.add_argument('--include-count', '-c', action='store_true',
                        help='Include repository count in legend labels')
    parser.add_argument('--plot-groups-total', '-p', action='store_true',
                        help='Add a line showing the total across all groups')
    parser.add_argument('--total-minimum', type=str, default='1GB',
                        help='Minimum total size to show on the plot (default: 1GB)')
    args = parser.parse_args()
    # Validate arguments
    if args.group is None and args.input_pattern is None:
        parser.error("Either --group or input_pattern must be provided")
    return args

def parse_size(size_str):
    """Convert a human-readable size string to bytes."""
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4,
        'PB': 1024**5
    }
    
    # Extract the number and unit
    size = size_str.strip()
    if size[-2:] in units:
        num = float(size[:-2])
        unit = size[-2:]
    elif size[-1:] in ['B']:
        num = float(size[:-1])
        unit = size[-1:]
    else:
        try:
            return float(size)
        except ValueError:
            raise ValueError(f"Invalid size format: {size_str}")
    
    return num * units.get(unit, 1)

def load_json_files(patterns):
    """Load all JSON files matching the patterns."""
    all_data = []
    file_count = 0
    # Handle single pattern or list of patterns
    if isinstance(patterns, str):
        patterns = [patterns]
    for pattern in patterns:
        for filename in glob.glob(pattern, recursive=True):
            try:
                with open(filename, 'r') as f:
                    data = json.load(f)
                    all_data.append(data)
                    file_count += 1
                print(f"Loaded {filename} with {len(data)} entries")
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    return all_data, file_count

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

def calculate_groups_total(group_data):
    """Calculate the total across all groups for each month."""
    all_months = set()
    for monthly_data in group_data.values():
        all_months.update(monthly_data.keys())
    total_data = {month: {'git_size': 0, 'annex_size': 0, 'total_size': 0} for month in all_months}
    for group_name, monthly_data in group_data.items():
        for month, sizes in monthly_data.items():
            total_data[month]['git_size'] += sizes['git_size']
            total_data[month]['annex_size'] += sizes['annex_size']
            total_data[month]['total_size'] += sizes['total_size']
    return total_data

def create_plot(group_data, repo_counts, output_filename, title, use_log_scale, show_components, include_count, plot_groups_total, min_total_size):
    """Create a plot of the monthly data with humanized size labels for multiple groups."""
    plt.figure(figsize=(12, 8))
    # Define a color cycle for the groups
    colors = plt.cm.tab10.colors
    color_cycle = itertools.cycle(colors)
    # Define line styles
    if show_components:
        styles = {
            'git_size': ':',      # dotted
            'annex_size': '--',   # dashed
            'total_size': '-'     # solid
        }
    else:
        styles = {
            'total_size': '-'     # solid only
        }
    
    # Keep track of all dates for x-axis
    all_dates = set()
    
    # Calculate total across all groups if requested
    if plot_groups_total and len(group_data) > 1:
        total_data = calculate_groups_total(group_data)
        # Add the total to the group data (it will be plotted last)
        group_data['Total (All Groups)'] = total_data
        repo_counts['Total (All Groups)'] = sum(repo_counts.values())
    
    # Find the first date when total size reaches the minimum for each group
    first_valid_dates = {}
    for group_name, monthly_data in group_data.items():
        sorted_months = sorted(monthly_data.keys())
        for month in sorted_months:
            if monthly_data[month]['total_size'] >= min_total_size:
                first_valid_dates[group_name] = datetime.strptime(month, '%Y-%m')
                break
    
    # Plot each group
    for group_idx, (group_name, monthly_data) in enumerate(group_data.items()):
        # Sort months chronologically
        sorted_months = sorted(monthly_data.keys())
        
        # Filter months based on minimum total size
        valid_months = []
        for month in sorted_months:
            if monthly_data[month]['total_size'] >= min_total_size:
                valid_months.append(month)
        
        if not valid_months:
            print(f"Group '{group_name}' has no data above the minimum size threshold")
            continue
            
        dates = [datetime.strptime(month, '%Y-%m') for month in valid_months]
        all_dates.update(dates)
        
        # Get the color for this group
        if "All Groups" in group_name:
            group_linewidth = 4
            group_color = '#A9A9A9'
        else:
            group_linewidth = 2
            group_color = next(color_cycle)
        
        # Create label with repository count if requested
        if include_count:
            base_label = f'{group_name} ({repo_counts[group_name]} repos)'
        else:
            base_label = group_name
        
        # Plot the components based on user preference
        if show_components:
            # Extract and plot git size
            git_sizes = [monthly_data[month]['git_size'] for month in valid_months]
            plt.plot(dates, git_sizes,
                     color=group_color, linestyle=styles['git_size'],
                     label=f'{base_label} - Git', linewidth=2)
            
            # Extract and plot annex size
            annex_sizes = [monthly_data[month]['annex_size'] for month in valid_months]
            plt.plot(dates, annex_sizes,
                     color=group_color, linestyle=styles['annex_size'],
                     label=f'{base_label} - Git-Annex', linewidth=2)
            
            # Extract and plot total size
            total_sizes = [monthly_data[month]['total_size'] for month in valid_months]
            plt.plot(dates, total_sizes,
                     color=group_color, linestyle=styles['total_size'],
                     label=f'{base_label} - Total', linewidth=2)
        else:
            # Only plot total size
            total_sizes = [monthly_data[month]['total_size'] for month in valid_months]
            plt.plot(dates, total_sizes,
                     color=group_color, linestyle=styles['total_size'],
                     label=base_label,
                     linewidth=group_linewidth,
                     )
    
    # Format the plot
    plt.title(title, fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Size', fontsize=14)
    
    # Set y-axis minimum to the minimum total size
    plt.ylim(bottom=min_total_size)
    
    # Use log scale if requested
    if use_log_scale:
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
    plt.legend(fontsize=10, loc='best')
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_filename, dpi=300)
    print(f"Plot saved to {output_filename}")
    
    # Also display it if running in an interactive environment
    plt.show()

def main():
    args = parse_args()
    
    # Parse the minimum total size
    min_total_size = parse_size(args.total_minimum)
    print(f"Using minimum total size: {humanize.naturalsize(min_total_size, binary=True)}")
    
    # Process groups or single pattern
    group_data = {}
    repo_counts = {}
    
    if args.group:
        # Process each group
        for group in args.group:
            group_name = group[0]
            group_patterns = group[1:]
            print(f"Processing group '{group_name}' with patterns: {group_patterns}")
            
            # Load JSON files for this group
            group_json_data, file_count = load_json_files(group_patterns)
            if group_json_data:
                # Aggregate data for this group
                group_data[group_name] = aggregate_by_month(group_json_data)
                repo_counts[group_name] = file_count
            else:
                print(f"No data found for group '{group_name}'")
    else:
        # Process single pattern (no groups)
        all_data, file_count = load_json_files(args.input_pattern)
        if all_data:
            group_data['All'] = aggregate_by_month(all_data)
            repo_counts['All'] = file_count
        else:
            print("No data found. Check the input pattern.")
            return
    
    # Check if we have any valid data
    if not group_data:
        print("No valid data found in any of the JSON files.")
        return
    
    # Create the plot
    create_plot(
        group_data,
        repo_counts,
        args.output,
        args.title,
        args.log_scale,
        args.separate_components,
        args.include_count,
        args.plot_groups_total,
        min_total_size
    )

if __name__ == '__main__':
    main()