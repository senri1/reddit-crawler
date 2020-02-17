from datetime import datetime, timedelta, timezone
from google.cloud import storage
import time
import requests
import json
import pandas as pd
import uuid
import time

def getPushshiftData(start, end, sub):

    reddit_posts = []
    # url to api endpoint
    url_template = ('https://api.pushshift.io/reddit/submission/search/'
                   '?subreddit={}&after={}&before={}&sort_type=created_utc&sort=asc&limit={}')
    info_template = ("Posts from {} to {} received.")
    # will retry 10 times before failing
    retries = 0
    retry_limit = 10

    # loop till start is less than end
    while(int(start) < int(end)):
    
        url = url_template.format(sub, str(start), end, 200)
        received = requests.get(url)
        
        if received.status_code != 200:
            if retries == retry_limit:
                print("Retry limit of {} reached, aborting scrape.".format(retry_limit))
                return reddit_posts, False
            else:
                retries += 1
                print("Error code : {}".format(received.status_code))
                print("Retrying")
                time.sleep(0.1)

        else:
            retries = 0    
            json_posts = json.loads(received.text)['data']
            if len(json_posts) != 0 :
                reddit_posts += json_posts
                print(info_template.format(datetime.utcfromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S'), datetime.utcfromtimestamp(reddit_posts[-1]['created_utc']).strftime('%Y-%m-%d %H:%M:%S')))
                start = reddit_posts[-1]['created_utc']

            # if data is empty all data is gathered
            else:
                return reddit_posts, True

    return reddit_posts, True


def crawlReddit(start_time, end_time, sub): 

    print("Getting reddit posts from {} to {}.\n".format(start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')))
    
    start_timestamp = int(start_time.replace(tzinfo=timezone.utc).timestamp())
    end_timestamp = int(end_time.replace(tzinfo=timezone.utc).timestamp())
    
    reddit_posts, success = getPushshiftData(start_timestamp, end_timestamp, sub)
    
    reddit_key_list = ["created_utc", "title", "selftext","subreddit", "id", "full_link", "retrieved_on" ]
    post_dict = {key: [] for key in reddit_key_list}

    for post in reddit_posts:
        for key in reddit_key_list:
            try:
                post_dict[key].append(post[key])
            except:
                post_dict[key].append("KEY NOT RETURNED BY API.")

    post_data = pd.DataFrame(post_dict)
    
    return post_data, success    

def uploadtoGCS(bucket_name, source_blob_path, destination_blob_path):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_path)

        blob.upload_from_filename(source_blob_path)
        return True
    except Exception as e:
        print(e)
        return False