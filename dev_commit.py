
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
from helpers import *




def get_df_from_widget_name(widget_name, existing_widgets):
    try:
        if widget_name in existing_widgets:
            print(f"Updating Index {widget_name}")
            df_arr = get_widget_updates(widget_name, existing_widgets[widget_name]['block_timestamp'])
        else:
            print(f"Creating Index {widget_name}")
            df_arr = get_widget_updates(widget_name)
        return pd.DataFrame(df_arr)
    except Exception as e:
        print(f"An error occurred: {e}")
        failed_widgets.append(widget_name)
        return None



def create_or_update_widget(widget_entry, existing_widgets, widget_path):
    signer_dir = widget_entry['signer_id']
    widget_name = widget_entry['widget_name']
    timestamp = widget_entry['block_timestamp']

    env = {
        'GIT_COMMITTER_NAME': signer_dir,
        'GIT_COMMITTER_EMAIL': signer_dir
    }

    # put this following if else inside a try except block
    # try:
    if widget_name in existing_widgets:
        print(f"Updating Index {widget_name}")

        df_arr = get_widget_updates(widget_name, existing_widgets[widget_name]['block_timestamp'])
        df = pd.DataFrame(df_arr)

    else:
        print(f"Creating Index {widget_name}")
        df_arr = get_widget_updates(widget_name)
        df = pd.DataFrame(df_arr)
    # except:
    #     # if there is an error, skip this widget and add it failed widgets list
    #     failed_widgets.append(widget_name)


    run_git_command(['git', 'add', '.'], widget_path, env=env)
    run_git_command(['git', 'commit', '-m', f'Update {widget_name} by {signer_dir} at {timestamp}', '--date', timestamp], widget_path, env=env)


def process_widgets(widget_names_list, existing_widgets, base_path):
    os.chdir(base_path)

    for widget_name in widget_names_list:
        df = get_df_from_widget_name(widget_name, existing_widgets)
        if df is None:
            continue

        df = df.sort_values(by=['block_timestamp'])
        data = df.to_dict('records')
        dev_widgets = defaultdict(list)

        for entry in data:
            dev_widgets[entry['signer_id']].append(entry)

        for near_dev, widget_entries in dev_widgets.items():
            widget_path = os.path.join(base_path, near_dev)

            if not os.path.exists(widget_path):
                os.makedirs(widget_path)

            os.chdir(widget_path)

            for widget_entry in widget_entries:
                create_or_update_widget(widget_entry, existing_widgets, widget_path)

        os.chdir(base_path)


base_path = os.environ['WIDGET_ROOT_DIR']



# Sort data by block_timestamp
snowflake_data = get_widget_names()
widget_names_list = [row['widget_name'] for row in snowflake_data]
widget_names_list = set(widget_names_list)
# widget_names_list = [name for name in widget_names_list if name]
# ad_hot, skip widgets in this list already:
existing_widgets = get_checkpoints(base_path)
failed_widgets = []


# Call the main function to process widgets
process_widgets(widget_names_list, existing_widgets, base_path)