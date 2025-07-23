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
BUCKET_NAME = "lastbucket-weather"  # replace with your GCP bucket

# Fetch RSS XML
response = requests.get(RSS_FEED_URL, headers={"User-Agent": "Mozilla/5.0"})
xml = response.text

try:
    json_data = xmltodict.parse(xml)
    json_payload = json.dumps(json_data)

    # Publish to topic
    publisher = pubsub_v1.PublisherClient()
    project_id = os.environ["exalted-kit-465905-n2"]
    topic_path = publisher.topic_path(project_id, TOPIC_NAME)

    try:
        publisher.create_topic(name=topic_path)
    except Exception:
        pass  # topic may already exist

    future = publisher.publish(topic_path, data=json_payload.encode("utf-8"))
    print(f"Published message ID: {future.result()}")

    # Create subscription
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, SUBSCRIPTION_NAME)
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
    except Exception:
        pass  # already exists

    # Pull a message
    response = subscriber.pull(subscription=subscription_path, max_messages=1)
    if response.received_messages:
        message_data = response.received_messages[0].message.data.decode("utf-8")
        print("Received:", message_data)

        # Convert JSON to TOML
        toml_data = toml.dumps(json.loads(message_data))

        # Upload to bucket
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        blob = bucket.blob(f"weather/{timestamp}.toml")
        blob.upload_from_string(toml_data)
        print("Uploaded to bucket.")

        # Acknowledge
        ack_id = response.received_messages[0].ack_id
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[ack_id])

except Exception as e:
    print("Error:", e)
