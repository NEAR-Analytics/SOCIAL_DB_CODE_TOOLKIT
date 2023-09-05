

import os
import pandas as pd
import subprocess
from collections import defaultdict

from query_engine.client import *
import time
# Read the CSV file
import json

# create a delay within a loop:
import uuid


def get_github_id(signed_id):
    if signed_id in dev_profiles:
        profile_data_raw = dev_profiles[signed_id]
        if len(profile_data_raw):
            profile_data = dev_profiles[signed_id]['profile_data']
            if 'github' in profile_data:
                return profile_data['github']

    return signed_id

# df = get_all_widget()


# Function to run git commands
def run_git_command(command, path='.', env=None):
    process = subprocess.Popen(command, cwd=path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print(stderr.decode())
    else:
        print(stdout.decode())


def commit_parse_date(date_string):
    formats = ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S.%fZ']
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            pass
    raise ValueError(f'time data {date_string} does not match any of the formats')


def find_files(root_dir, file_name):
    file_paths = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        for file in filenames:
            if file == file_name:
                file_path = os.path.join(dirpath, file)
                file_paths.append(file_path)

    return file_paths

def get_checkpoints(root_directory):
    target_file_name = 'commit_raw.json'

    checkpoints = {}

    files_found = find_files(root_directory, target_file_name)

    if files_found:
        print(f"Found {len(files_found)} {target_file_name} files:")
        for file_path in files_found:
            # open json file
            with open(file_path) as json_file:
                data = json.load(json_file)
                # print(data)
                # print(data['widget_name'])
                # print(data['source_code'])
                # print(data['block_timestamp'])
                # print(data['signer_id'])
                # print(data['block_height'])
                # print(data['block_hash'])
                # print(
            checkpoints[data['widget_name']] = data
            print(file_path)
    else:
        print(f"No {target_file_name} files found in {root_directory}")
    return checkpoints
