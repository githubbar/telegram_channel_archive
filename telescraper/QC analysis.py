#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import os
import statistics as stat
from datetime import datetime
import operator
import re
import sys


# ## Quality Data Test Cases:
# 1. All messages that have images in Telegram, should have the correct number of images downloaded and recorded in json file
# 2. All downloaded images should have the correct corresponding ID of the post that stores text, emoji, other metadata in Telegram. [for each dowloaded file the file ID matches the Post ID. For each Post ID there is dowloaded file with corresponding ID]
# 3. All images recorded in files should have downloaded images in folders
# 4. All messages that have emojis on Telegram should have emojis in files

# ## QA/QC 
# The script to get the numeric stats for the data collection
# TODO: make this a table instead of the txt file

# In[ ]:


def count_metrics(data):
    counts = {"none_views_post": 0, "none_fwd_post": 0, "url_new": 0, "url_old": 0, "url": 0, "total_url_from_text": 0,
              "post_with_url": 0, "total_fwd_posts": 0, "total_views": 0, "total_fwds": 0,
             "lowest_views": float('inf'), "highest_views": float('-inf'), 
              "lowest_fwds": float('inf'), "highest_fwds": float('-inf'),
             "post_with_views": 0, "average_views": 0.0, 
             "lowest_text_length": float('inf'), "highest_text_length": float('-inf')}

    url_pattern = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

    for i in data.keys():
        post = data[i]
            
        if post.get('total_views') == None:
            print(f"message ID {post['id']} has no view count.")
            counts["none_views_post"] += 1
        else:
            views = post.get('total_views')
            counts["total_views"] += views
            counts["post_with_views"] += 1
            
            if views < counts["lowest_views"]:
                counts["lowest_views"] = views
            if views > counts["highest_views"]:
                counts["highest_views"] = views

        if post.get('total_fwds') == None:
            counts["none_fwd_post"] += 1
        else:
            fwds = post.get('total_fwds')
            counts["total_fwds"] += fwds
            if fwds < counts["lowest_fwds"]:
                counts["lowest_fwds"] = fwds
            if fwds > counts["highest_fwds"]:
                counts["highest_fwds"] = fwds
                
        if any('url' in item for item in post.get('media', [])):
            counts["url_new"] += 1
            counts["url"] += 1

        if 'url' in post:
            counts["url_old"] += 1
            counts["url"] += 1

        if 'text' in post and isinstance(post['text'], str):
            urls_in_text = url_pattern.findall(post['text'])
            if urls_in_text:
#                 print(post['id'], post['text'])
                counts["total_url_from_text"] += len(urls_in_text)
                counts["post_with_url"] += 1
        if 'fwd' in post:
            counts["total_fwd_posts"] += 1
        
        if 'text' in post and isinstance(post['text'], str):
            text_length = len(post['text'])
            if text_length < counts["lowest_text_length"]:
                counts["lowest_text_length"] = text_length
            if text_length > counts["highest_text_length"]:
                counts["highest_text_length"] = text_length
    
    # average views
    if counts["post_with_views"] > 0:
        counts["average_views"] = counts["total_views"] / counts["post_with_views"]

    return counts


# In[ ]:


sys.stdout = open('QC.txt', 'w')

main_folder = ""

period_totals = {}


# specify the order for the graph
period_order = ["011522-030122", "040122-050122", "060122-070122", "090122-101622", 
                "110122-120122", "020123-030123", "060123-070723"]

for folder_name in os.listdir(main_folder):
#     if folder_name == "eshkinkrot":
        folder_path = os.path.join(main_folder, folder_name)

        if os.path.isdir(folder_path):
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)

                if file_name.endswith(".json") and file_name != 'channel_info.json':
                    with open(file_path, "r") as json_file:
                        data = json.load(json_file)
                    num_posts = len(data)
                    period = str(file_name.rsplit('_', 1)[-1].rsplit('.', 1)[0])
                    print(f"Folder: {folder_name}, Period: {period}, Num Posts: {num_posts}")

                    metrics = count_metrics(data)
                    print("QA/QC metrics: ")
                    for key, value in metrics.items():
                        if key not in ["url_new", "url_old"]:
                            print(f"  {key}: {value}")
                    print()
sys.stdout.close()
sys.stdout = sys.__stdout__  # reset output to console


# In[ ]:


# sys.stdout = open('QC.txt', 'w')

main_folder = ""

period_totals = {}


# specify the order for the graph
period_order = ["011522-030122", "040122-050122", "060122-070122", "090122-101622", 
                "110122-120122", "020123-030123", "060123-070723"]

# Function to check if a given ID exists in the provided JSON data
def id_exists_in_data(id, data):
    for i in data.keys():
        post = data[i]
        if post.get('id') == id:
            return True
    return False

for folder_name in os.listdir(main_folder):
        print(folder_name)
        folder_path = os.path.join(main_folder, folder_name)
        image_folder_path = os.path.join(folder_path, "images")
        doc_folder_path = os.path.join(folder_path, "documents")
        extracted_file_ids = []
        if os.path.isdir(folder_path):
            # Images and Docs 
            if os.path.isdir(image_folder_path) and os.path.isdir(doc_folder_path) :
                for file_name in os.listdir(image_folder_path)+os.listdir(doc_folder_path):
                    if "photo" in file_name or "doc" in file_name:
                        # Extract the number that goes after "photo" or "doc" from each filename
                        try:
                            id_str = file_name.split("photo")[1].split(".")[0] if "photo" in file_name else file_name.split("doc")[1].split(".")[0]
                            extracted_file_ids.append(int(id_str))
                        except (IndexError, ValueError):
                            print(f"Failed to extract ID from {file_name}")
            # Go over the messages json files
            json_files = [file for file in os.listdir(folder_path) if file.endswith(".json") and file_name != 'channel_info.json']
            missing_associations = []
            for extracted_id in extracted_file_ids:
                found = False
                for file_name in json_files:
                    with open(os.path.join(folder_path, file_name), 'r') as json_file:
                        data = json.load(json_file)
                    if id_exists_in_data(extracted_id, data):
                        found = True
                        break
                if not found:
                    missing_associations.append(extracted_id)
        # Print or process the missing associations
        print(f"Folder: {folder_name}, Missing: {missing_associations}")
        print()


# In[ ]:




