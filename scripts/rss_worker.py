import requests
import xmltodict
import json
import toml
import time
import uuid
import os

# Constants
RSS_URL = "https://rss.accuweather.com/rss/liveweather_rss.asp?metric=1&locCode=ASI%7CIN%7CKA%7CBENGALURU"
TOPIC = "weather-updates"
BUCKET = " weather-xml-json-toml-storage"
TMP_JSON = "/tmp/weather.json"
TMP_TOML = f"/tmp/weather_{uuid.uuid4().hex}.toml"

# Step 1: Fetch XML data
response = requests.get(RSS_URL)
xml = response.content

# Step 2: Convert XML to JSON
json_data = xmltodict.parse(xml)
with open(TMP_JSON, "w") as f:
    json.dump(json_data, f)

# Step 3: Publish JSON to Pub/Sub
os.system(f"gcloud pubsub topics publish {TOPIC} --message \"$(cat {TMP_JSON} | base64)\"")

# Step 4: Pull message from subscription
output = os.popen("gcloud pubsub subscriptions pull weather-sub --auto-ack --limit=1 --format='value(message.data)'").read().strip()
decoded_json = json.loads(base64.b64decode(output.encode()).decode())

# Step 5: Convert to TOML and save
with open(TMP_TOML, "w") as f:
    f.write(toml.dumps(decoded_json))

# Step 6: Upload to GCS
os.system(f"gcloud storage cp {TMP_TOML} gs://{BUCKET}/")
