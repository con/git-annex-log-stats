#!/usr/bin/env python3
import asyncio
import subprocess
import json
import os
from datetime import datetime
import git
from tqdm import tqdm
import aiofiles

async def get_annex_size_async(repo_path, commit):
    """Returns the size of annexed files for a given commit asynchronously."""
    cmd = ['git', '-C', repo_path, 'annex', 'info', '--bytes', '--json', commit]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            print(f"Error retrieving annex size for commit {commit}: {stderr}")
            return 0
            
        annex_info = json.loads(stdout.decode())
        return int(annex_info.get('size of annexed files in tree', 0))
    except Exception as e:
        print(f"Error retrieving annex size for commit {commit} using {' '.join(cmd)} : {e}")
        return 0

async def process_commit(repo_path, commit, results, output_filename):
    """Process a single commit and update results."""
    try:
        # Get commit timestamp and format it
        commit_time = datetime.fromtimestamp(commit.committed_date)
        commit_time_str = commit_time.isoformat()
        
        # Get the size of annexed files
        annex_size = await get_annex_size_async(repo_path, commit.hexsha)
        
        # Get the total size of the git objects
        git_size = sum((item.size for item in commit.tree.traverse() 
                        if item.type == 'blob' and item.mode != 0o120000), 0)
        
        # Store the results
        results[commit.hexsha] = {
            'timestamp': commit_time_str,
            'git_size': git_size,
            'annex_size': annex_size,
            'total_size': git_size + annex_size
        }
        
        # Save the updated results
        await write_json_async(output_filename, results)
        
    except Exception as e:
        print(f"Error processing commit {commit.hexsha}: {e}")

async def write_json_async(output_filename, data):
    """Writes the data to a JSON file asynchronously."""
    async with aiofiles.open(output_filename, 'w') as f:
        await f.write(json.dumps(data, indent=4))

async def load_existing_results(output_filename):
    """Load existing results from file if it exists."""
    if os.path.exists(output_filename):
        try:
            async with aiofiles.open(output_filename, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            print(f"Error loading existing results: {e}")
    return {}

async def get_git_and_annex_sizes_async(repo_path, output_filename):
    """Traverse the git history and gather size data asynchronously."""
    repo = git.Repo(repo_path)
    
    # Load existing results to avoid reprocessing
    results = await load_existing_results(output_filename)
    
    # Get all commits
    all_commits = list(repo.iter_commits())
    
    # Filter out already processed commits
    commits_to_process = [commit for commit in all_commits if commit.hexsha not in results]
    
    print(f"Total commits: {len(all_commits)}")
    print(f"Already processed: {len(all_commits) - len(commits_to_process)}")
    print(f"Commits to process: {len(commits_to_process)}")
    
    # Process commits with a progress bar
    for commit in tqdm(commits_to_process, desc="Processing commits"):
        await process_commit(repo_path, commit, results, output_filename)
    
    return results

async def main_async(repo_path, output_filename):
    """Main async function."""
    results = await get_git_and_annex_sizes_async(repo_path, output_filename)
    print(f"Processed {len(results)} commits. Results saved to {output_filename}")

def main(repo_path, output_filename):
    """Main function that runs the async event loop."""
    asyncio.run(main_async(repo_path, output_filename))

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <repo_path> <output_filename>")
        sys.exit(1)
    
    repo_path = sys.argv[1]
    output_filename = sys.argv[2]
    main(repo_path, output_filename)
