from scrape import *
from google.cloud import storage
import uuid

def main():
    start_time = (2020, 2, 12, 12, 0, 0)
    end_time = (2020, 2, 13, 13, 29, 0)
    sub = "Vodafone"
    file_name = "reddit_test_{}.csv".format(uuid.uuid4())
    bucket_name = "g09-datasets"
    file_name = "reddit_test_{}.csv".format(uuid.uuid4())   # change to unique name with datetime.
    source_blob_path = "/tmp/" + file_name
    destination_blob_path = "reddit/crawl/" + file_name
    subbreddit_name = 'RocketLeague'

    print("Bucket name: {}".format(bucket_name))
    print("Bucket destination path: {}".format(destination_blob_path))
    print("Subbreddit: {}".format(subbreddit_name))

    df = crawlReddit(start_time, end_time, sub)
    scraped_data.to_csv(source_blob_path)
    succeeded = uploadtoGCS(bucket_name, source_blob_path, destination_blob_path)

    print(succeeded)


def uploadtoGCS(bucket_name, source_blob_path, destination_blob_path):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json('./cogniflare-rd-1298fa5958c1.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_path)

    blob.upload_from_filename(source_blob_path)
    return True

if __name__ == '__main__':
    main()
