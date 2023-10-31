from dotenv import load_dotenv
import os
import praw
from urllib.parse import quote_plus
import openai
import datetime

load_dotenv()

eli5_model = 'ft:gpt-3.5-turbo-0613:personal::8EpKrng8'
eli5_system_message = 'You are a friendly, knowledgeable Reddit user explaining a concept in simple terms.'
openai.api_key = os.getenv('OPENAI_API_KEY')

min_post_score = 8 # minimum score (upvotes-downvotes) for a post to be replied to
max_post_comments = 4 # maximum number of comments for a post to be replied to
min_post_age = 60*5 # minimum age of post in seconds to be replied to (10 minutes)
max_post_age = 60*60*3 # maximum age of post in seconds to be replied to (3 hours)

def main():

    # api credentials
    reddit = praw.Reddit(
        client_id = os.getenv('REPLYER_CLIENT_ID'),
        client_secret = os.getenv('REPLYER_CLIENT_SECRET'),
        user_agent = os.getenv('REPLYER_USER_AGENT'),
        username = os.getenv('REPLYER_REDDIT_USERNAME'),
        password = os.getenv('REPLYER_REDDIT_PASSWORD'),
    )

    # listen for new posts and reply to them
    subreddit = reddit.subreddit('explainlikeimfive')
    for submission in subreddit.stream.submissions():
        # trying to select posts that are interesting, relatively new and not flooded with replies
        if submission.score >= min_post_score and submission.num_comments <= max_post_comments and datetime.datetime.now().timestamp() - submission.created_utc >= min_post_age and datetime.datetime.now().timestamp() - submission.created_utc <= max_post_age:
            process_submission(submission)


def process_submission(submission):

    prompt = submission.title + '\n\n' + submission.selftext
    reply_text = generate_reply(prompt, eli5_model, eli5_system_message)
    if reply_text:
        print(f"Replying to: {submission.title}\n{submission.url}")
        submission.reply(reply_text)
    else:
        print(f"Failed to reply to: {submission.title}")


def generate_reply(prompt, model_id, system_instruction=None, max_tokens=512):
    messages = []
    
    if system_instruction:
        messages.append({"role": "system", "content": system_instruction})
    
    messages.append({"role": "user", "content": prompt})

    response = openai.ChatCompletion.create(
        model=model_id,
        messages=messages,
        max_tokens=max_tokens,
        n=1,
        stop=None,
        temperature=0.9,
    )

    assistant_reply = response['choices'][0]['message']['content'].strip()

    # check if the reply is complete by looking for end of sentence marker (somewhat accurate)
    if assistant_reply[-1] in ['.', '?', '!']:
        return assistant_reply
    else:
        return False


if __name__ == '__main__':
    main()