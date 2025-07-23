import requests
import xmltodict
import json
import toml
import os
from datetime import datetime
from google.cloud import pubsub_v1, storage

# CONFIG
RSS_FEED_URL = "https://rss.accuweather.com/rss/liveweather_rss.asp?metric=1&locCode=ASI|IN|KA|BENGALURU"
TOPIC_NAME = "weather-topic"
SUBSCRIPTION_NAME = "weather-subscription"
BUCKET_NAME = "lastbucket-weather"

# Fetch RSS XML
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(RSS_FEED_URL, headers=headers)

if response.status_code != 200:
    print("Failed to fetch RSS feed:", response.status_code)
    exit(1)

xml = response.text

try:
    # Convert to JSON
    json_data = xmltodict.parse(xml)
    json_payload = json.dumps(json_data)

    # Google Cloud Setup
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable not set.")

    # Publish to Pub/Sub Topic
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, TOPIC_NAME)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"Created topic: {TOPIC_NAME}")
    except Exception as e:
        print(f"Topic may already exist: {e}")

    future = publisher.publish(topic_path, data=json_payload.encode("utf-8"))
    print(f"Published message ID: {future.result()}")

    # Create Subscription
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, SUBSCRIPTION_NAME)

    try:
        subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print(f"Created subscription: {SUBSCRIPTION_NAME}")
    except Exception as e:
        print(f"Subscription may already exist: {e}")

    # Pull Message
    response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 1})
    if not response.received_messages:
        print("No messages received from subscription.")
        exit(0)

    message_data = response.received_messages[0].message.data.decode("utf-8")
    print("Received message from subscription.")

    # Convert to TOML
    toml_data = toml.dumps(json.loads(message_data))

    # Upload to GCS Bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    if not bucket.exists():
        print(f"Bucket '{BUCKET_NAME}' not found. Please create it in GCP.")
        exit(1)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    blob = bucket.blob(f"weather/{timestamp}.toml")
    blob.upload_from_string(toml_data)
    print(f"Uploaded weather update to bucket: {blob.name}")

    # No Acknowledgement (as per your request)

except Exception as e:
    print("Error:", e)
