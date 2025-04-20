import requests

# Set the URL for the API
url_last_minute = 'http://127.0.0.1:8086/last-minute'
url_last_30_minutes = 'http://127.0.0.1:8086/last-30-minutes'

# Make a GET request to the last-minute endpoint
response_last_minute = requests.get(url_last_minute)
if response_last_minute.status_code == 200:
    print("Last Minute Data:", response_last_minute.json())
else:
    print(f"Failed to get last-minute data. Status Code: {response_last_minute.status_code}")

# Make a GET request to the last-30-minutes endpoint
response_last_30_minutes = requests.get(url_last_30_minutes)
if response_last_30_minutes.status_code == 200:
    print("Last 30 Minutes Data:", response_last_30_minutes.json())
else:
    print(f"Failed to get last-30-minutes data. Status Code: {response_last_30_minutes.status_code}")
