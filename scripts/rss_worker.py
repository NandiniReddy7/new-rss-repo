import requests
import xmltodict
import json

rss_url = "https://rss.accuweather.com/rss/liveweather_rss.asp?metric=1&locCode=ASI%7CIN%7CKA%7CBENGALURU"
response = requests.get(rss_url)
xml = response.text

# Print the raw XML for debugging
print("==== RAW XML RESPONSE ====")
print(xml)

# Save to a temporary file (optional)
with open("raw_feed.xml", "w") as f:
    f.write(xml)

# Try parsing
try:
    json_data = xmltodict.parse(xml)
except Exception as e:
    print("Error while parsing XML:", e)
    exit(1)

# Convert to JSON and print
print("==== JSON Data ====")
print(json.dumps(json_data, indent=2))
