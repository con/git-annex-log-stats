#!/usr/bin/env python3

import subprocess
import json
from datetime import datetime
from collections import defaultdict
import git
from tqdm import tqdm

def get_annex_size(repo_path, commit):
    """Returns the size of annexed files for a given commit."""
    cmd = ['git', '-C', repo_path, 'annex', 'info', '--bytes', '--json', commit]
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
    except Exception as e:
        print(f"Error retrieving annex size for commit {commit} using {' '.join(cmd)} : {e}")
        return 0
    annex_info = json.loads(result.stdout)
    return int(annex_info['size of annexed files in tree'])

def get_git_and_annex_sizes(repo_path):
    """Traverse the git history and gather size data."""
    repo = git.Repo(repo_path)
    sizes = defaultdict(lambda: {'git': 0, 'annex': 0})

    for commit in tqdm(repo.iter_commits()):
        date = datetime.fromtimestamp(commit.committed_date)
        month_key = date.strftime('%Y-%m')
        try:
            # Checkout the commit to get proper info in the working tree.
            # repo.git.checkout(commit)

            # Get the size of annexed files
            annex_size = get_annex_size(repo_path, commit.hexsha)

            # Get the total size of the git objects
            git_size = sum(
                (blob.size for blob in commit.tree.traverse()),
                0,
            )
        
            total_size = git_size + annex_size
            if total_size > (sizes[month_key]['git'] + sizes[month_key]['annex']):
                sizes[month_key] = {'git': git_size, 'annex': annex_size}

        except Exception as e:
            print(f"Error processing commit {commit}: {e}")

    # Restore the original head
    repo.git.checkout('HEAD')

    return sizes

def write_json(output_filename, data):
    """Writes the size data to a JSON file."""
    with open(output_filename, 'w') as f:
        json.dump(data, f, indent=4)

def main(repo_path, output_filename):
    sizes = get_git_and_annex_sizes(repo_path)
    write_json(output_filename, sizes)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <repo_path> <output_filename>")
        sys.exit(1)

    repo_path = sys.argv[1]
    output_filename = sys.argv[2]
    main(repo_path, output_filename)
