import requests
import boto3

BUCKET = "sports-injury-pipeline-manav"
s3 = boto3.client("s3")

# NFL injury reports — weekly data, good volume
url = "https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_2023.csv"
r = requests.get(url)

# Upload raw — no transformation, exactly as downloaded
s3.put_object(
    Bucket=BUCKET,
    Key="raw/nfl/injuries/injuries_2023.csv",
    Body=r.content
)
print("Uploaded NFL injuries 2023")

# Also grab 2022 for multi-year depth
url2 = "https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_2022.csv"
r2 = requests.get(url2)
s3.put_object(
    Bucket=BUCKET,
    Key="raw/nfl/injuries/injuries_2022.csv",
    Body=r2.content
)
print("Uploaded NFL injuries 2022")