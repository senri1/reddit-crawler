import time
from datetime import datetime
import requests
import json
import pandas as pd
import uuid

def getPushshiftData(start, end, sub):

    reddit_posts = []
    url_template = ('https://api.pushshift.io/reddit/submission/search/'
                   '?subreddit={}&after={}&before={}&sort_type=created_utc&sort=asc&limit={}')

    while(int(start) < int(end)):

        url = url_template.format(sub, str(start), end, 200)
        received = requests.get(url)
        json_posts = json.loads(received.text)['data']

        if len(json_posts) != 0 :
            reddit_posts += json_posts
            start = reddit_posts[-1]['created_utc']
        else:
            return reddit_posts

    return reddit_posts


def datetime_to_timestamp(time):

    dt = datetime(time[0], time[1], time[2], time[3], time[4], time[5]) 
    unix_timestamp = dt.strftime('%s')

    return unix_timestamp


def crawlReddit(start_time, end_time, sub): 

    start_timestamp = datetime_to_timestamp(start_time)
    end_timestamp = datetime_to_timestamp(end_time)
    
    print("Getting reddit posts from {} to {}.\n".format(start_time, end_time))
    reddit_posts = getPushshiftData(start_timestamp, end_timestamp, sub)
    
    reddit_key_list = ["created_utc", "title", "selftext","subreddit", "id", "full_link", "retrieved_on" ]
    post_dict = {key: [] for key in reddit_key_list}

    for post in reddit_posts:
        for key in reddit_key_list:
            try:
                post_dict[key].append(post[key])
            except:
                post_dict[key].append("KEY NOT RETURNED BY API.")

    post_data = pd.DataFrame(post_dict)
    
    return post_data    
