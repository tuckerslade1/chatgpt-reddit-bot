import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
user_agent = os.getenv('USER_AGENT')
minimum_score = 1200

for i in range(17):
    if i < 10:
        data = pd.read_parquet('../chatgpt-reddit-bot-data/eli5-0' + str(i) + '.parquet', columns=['author', 'body', 'created_utc', 'edited', 'parent_id', 'score'])
    else:
        data = pd.read_parquet('../chatgpt-reddit-bot-data/eli5-' + str(i) + '.parquet', columns=['author', 'body', 'created_utc', 'edited', 'parent_id', 'score'])

    cleaned_data = pd.DataFrame(columns=['author', 'body', 'created_utc', 'edited', 'parent_id', 'score'])




    # cleaning data

    # remove messages with score < minimum_score
    data['score'] = data['score'].astype(float) # ensure 'score' column is of type int or float
    cleaned_data = data[data['score'] >= minimum_score] # filter low scoring messages

    # remove messages that are not top level comments
    # (parent_id starts with t3)
    cleaned_data = cleaned_data[cleaned_data['parent_id'].str[:2] == 't3']

    # remove edited messages
    cleaned_data = cleaned_data[cleaned_data['edited'] == 'False']

    # remove deleted messages
    cleaned_data = cleaned_data[cleaned_data['body'] != '[deleted]']

    # remove messages created by automoderator
    cleaned_data = cleaned_data[cleaned_data['author'] != 'AutoModerator']

    # create output json file
    if i < 10:
        cleaned_data.to_json('cleaned_data_0' + str(i) + '.json', orient='records', lines=True)
    else:
        cleaned_data.to_json('cleaned_data_' + str(i) + '.json', orient='records', lines=True)

    print('Finished cleaning data for file ' + str(i+1) + ' of 17')