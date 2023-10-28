import pandas as pd

minimum_score = 100

data = pd.read_parquet('../chatgpt-reddit-bot-data/eli5-00.parquet', columns=['author', 'body', 'created_utc', 'edited', 'parent_id', 'score'])
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
cleaned_data.to_json('cleaned_data_00.json', orient='records', lines=True)