import pandas as pd
import requests
import os
import time
from dotenv import load_dotenv

# data source: https://huggingface.co/datasets/HuggingFaceGECLM/REDDIT_comments

load_dotenv()

# load reddit api credentials from .env file
client_id = os.getenv('CLEANER_CLIENT_ID')
client_secret = os.getenv('CLEANER_CLIENT_SECRET')
user_agent = os.getenv('CLEANER_USER_AGENT')
username = os.getenv('CLEANER_REDDIT_USERNAME')
password = os.getenv('CLEANER_REDDIT_PASSWORD')

minimum_score = 1000 # minimum score (upvotes-downvotes) for a comment to be included in the dataset
subreddit_name = input('Enter subreddit name (ex. explainlikeimfive):\n')
subreddit_path = f'../chatgpt-reddit-bot-data/{subreddit_name}'
num_parquets = 10

def format_row(row):
    return {"messages": [{"role": "system", "content": "You are a friendly, knowledgeable Reddit user explaining a concept in simple terms."},{"role": "user", "content": row['parent_content']},{"role": "assistant", "content": row['body']}]
}

# clean and obtain parent content for each file
print('Cleaning data...')

for file_num in range(num_parquets):
    data = pd.read_parquet(f'{subreddit_path}/parquets/{subreddit_name}-{str(file_num).zfill(2)}.parquet', columns=['author', 'body', 'created_utc', 'edited', 'parent_id', 'score'])

    # create empty dataframe to store cleaned data
    cleaned_data = pd.DataFrame(columns=['author', 'body', 'created_utc', 'edited', 'parent_id', 'score'])



    # INITIAL CLEANING OF DATA

    # remove messages with score < minimum_score
    data['score'] = data['score'].astype(float) # ensure 'score' column is of type int or float
    cleaned_data = data[data['score'] >= minimum_score] # filter low scoring messages

    # remove messages that are not top level comments (parent_id starts with t3)
    cleaned_data = cleaned_data[cleaned_data['parent_id'].str[:2] == 't3']

    # remove edited messages
    cleaned_data = cleaned_data[cleaned_data['edited'] == 'False']

    # remove deleted messages
    cleaned_data = cleaned_data[cleaned_data['body'] != '[deleted]']
    cleaned_data = cleaned_data[cleaned_data['body'] != '[removed]']

    # remove messages created by automoderator
    cleaned_data = cleaned_data[cleaned_data['author'] != 'AutoModerator']

    # dropping all columns except body and parent id
    cleaned_data = cleaned_data.drop(columns=['author', 'created_utc', 'edited', 'score'])

    # creating new column for parent content
    cleaned_data['parent_content'] = ''




    # OBTAINING CONTENT OF PARENT POSTS

    # Get the access token
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {'grant_type': 'password', 'username': username, 'password': password}
    headers = {'User-Agent': user_agent}
    response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
    token = response.json().get('access_token')
    headers = {'Authorization': f'bearer {token}', 'User-Agent': f'{user_agent}/0.0.1'}

    # Extract all parent post IDs
    post_ids = cleaned_data['parent_id'].str[0:].tolist()

    # Split the list of post IDs into chunks of 100 (or less) to avoid exceeding Reddit API rate limit
    post_id_chunks = [post_ids[i:i+100] for i in range(0, len(post_ids), 100)]

    # Dictionary to store the content of each post
    post_content_dict = {}

    # fetch the content of the parent posts in batches of 100
    for post_id_chunk in post_id_chunks:
        time.sleep(2)  # sleep for 2 seconds to avoid exceeding Reddit API rate limit
        combined_post_ids = ",".join(post_id_chunk)
        url = f'https://oauth.reddit.com/by_id/{combined_post_ids}'
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            posts_data = response.json()['data']['children']
            for post in posts_data:
                post_id = post['data']['id']
                post_body = post['data']['selftext']
                post_title = post['data']['title']

                if post['data']['is_self']:
                    if post_body:  # If post_body is not empty
                        post_content_dict[post_id] = post_title + ". " + post_body
                    else:  # If post_body is empty
                        post_content_dict[post_id] = post_title
                else:
                    post_content_dict[post_id] = 'bad_parent'

        elif response.status_code == 404:
            print(f"Post with ID {post_id} not found.")
            continue
        else:
            print(f"Error with status code: {response.status_code}")
            continue # skip to next post_id_chunk

    # Map the content back to the dataframe
    cleaned_data['parent_content'] = cleaned_data['parent_id'].str[3:].map(post_content_dict)
    



    # FINAL CLEANING OF DATA

    # delete rows where parent content is not selftext
    cleaned_data = cleaned_data[cleaned_data['parent_content'] != 'bad_parent']

    # delete rows with empty parent content
    cleaned_data = cleaned_data[cleaned_data['parent_content'] != '']

    # filter out null values
    cleaned_data = cleaned_data[cleaned_data['parent_content'].notna()]

    # delete rows with removed posts
    cleaned_data = cleaned_data[~cleaned_data['parent_content'].str.contains("\[deleted by user\]|\[removed\]|\[deleted\]", na=False, regex=True)]

    # delete parent_id column
    cleaned_data = cleaned_data.drop(columns=['parent_id'])

    cleaned_data['formatted_data'] = cleaned_data.apply(format_row, axis=1)


    # create output json file
    cleaned_data['formatted_data'].to_json(f'{subreddit_path}/cleaned_data/cleaned_data_{str(file_num).zfill(2)}.jsonl', orient='records', lines=True)
    print('Finished cleaning data for file ' + str(file_num+1) + ' of ' + str(num_parquets))

# aggregate all json files into one
filenames = [f'{subreddit_path}/cleaned_data/cleaned_data_{str(i).zfill(2)}.jsonl' for i in range(num_parquets)]
combined_filename = f'{subreddit_path}/cleaned_data/aggregate_data.jsonl'

# Combining the files
with open(combined_filename, 'w') as combined_file:
    for filename in filenames:
        with open(filename, 'r') as f:
            for line in f:
                combined_file.write(line)