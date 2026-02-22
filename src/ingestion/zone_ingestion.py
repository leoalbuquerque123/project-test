import requests

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
output_path = "taxi_zone_lookup.csv"

response = requests.get(url)
response.raise_for_status()

with open(output_path, "wb") as f:
    f.write(response.content)

print("Download !")