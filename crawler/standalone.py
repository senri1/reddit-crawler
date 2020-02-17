from scrape import crawlReddit, uploadtoGCS
from datetime import date, datetime, timedelta, timezone
import uuid
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "crawler-service-account-key.json"
bucket_name = "g09-datasets"
sub = "tmobile"
day_period = 365
endDateTime = datetime.utcnow()
startDateTime = endDateTime - timedelta(days = day_period)

file_name = "{}_{}.csv".format(startDateTime.strftime('%Y-%m-%d-%H-%M-%S'), endDateTime.strftime('%Y-%m-%d-%H-%M-%S'))
source_blob_path = "/tmp/" + file_name
destination_blob_path = "reddit/crawl/{}/{}".format(sub, file_name)
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
