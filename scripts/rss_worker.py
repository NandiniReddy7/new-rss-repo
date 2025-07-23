import requests
import xmltodict
import json

rss_url = "https://rss.accuweather.com/rss/liveweather_rss.asp?metric=1&locCode=ASI%7CIN%7CKA%7CBENGALURU"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

response = requests.get(rss_url, headers=headers)
xml = response.text

# Debug print
print("==== RAW XML RESPONSE ====")
print(xml[:500])  # Only first 500 chars

# Try parsing
try:
    json_data = xmltodict.parse(xml)
except Exception as e:
    print("Error while parsing XML:", e)
    exit(1)

# Convert to JSON
json_output = json.dumps(json_data, indent=2)
print("==== JSON DATA ====")
print(json_output)
