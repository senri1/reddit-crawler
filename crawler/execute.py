from scrape import crawlReddit, uploadtoGCS
from db import RedditLogDB
from datetime import date, datetime, timedelta, timezone
import uuid
import os

def main():
    # Set vairables
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "crawler-service-account-key.json"
    bucket_name = "g09-datasets"
    sub = "Vodafone"
    day_period = 1

    # Initalise MySQL db object
    logDB = RedditLogDB(host='104.196.70.209', user='k8s', password='', db='reddit', tablename='reddit_log')

    # Evaluate crawl period, if empty use current time
    endDateTime = logDB.earliestQuery()
    if not endDateTime:
        endDateTime = datetime.utcnow()
    else:
        # For some reason it returns a tuple if its not empty
        endDateTime = endDateTime[0]
        
    startDateTime = endDateTime - timedelta(days = day_period)

    # Log data
    success, logID = logDB.startCrawl(startDateTime, endDateTime, sub)

    # Set relevant directories
    file_name = "{}_{}.csv".format(startDateTime.strftime('%Y-%m-%d-%H-%M-%S'), endDateTime.strftime('%Y-%m-%d-%H-%M-%S'))
    source_blob_path = "/tmp/" + file_name
    destination_blob_path = "reddit/crawl/{}/{}".format(sub, file_name)

    # Crawl subreddit
    print("Beginning crawl.")
    df, success_scrape = crawlReddit(startDateTime, endDateTime, sub)
    if success_scrape:
        print("Successfully finished crawl.")
    else:
        print("Crawl failed.")

    # Save dataframe to csv
    df.to_csv(source_blob_path)

    # Upload to GCS
    print("Uploading to GCS.")
    success_to_GCS = uploadtoGCS(bucket_name, source_blob_path, destination_blob_path)
    if success_to_GCS:
        print("Successfully uploaded to GCS.")
    else:
        print("Failed to upload to GCS.")

    # Log data
    logDB.endCrawl(logID, len(df), (success_scrape and success_to_GCS) )

if __name__ == "__main__":
    main()