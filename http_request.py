import requests
import os

endpoint_url = "https://eb8e3c6481d4463fafe4833aec3119d8.eu-central-1.aws.cloud.es.io:9243/"
authorization = "Basic ZWxhc3RpYzpUM1FaT1VMellmdHdWSktFamxlQ05yenk="


def send_data(data):
    params = (
        ('pretty', ''),
    )
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authorization
    }
    response = requests.post(f"{endpoint_url}/_bulk", headers=headers, params=params, data=data)
    assert (200 <= response.status_code < 300)


for dirname, _, filenames in os.walk("data/ndjson"):
    for filename in filenames:
        if filename.split('.')[-1] == "ndjson":
            with open(os.path.join(dirname, filename), "r") as f:
                send_data(f.read())
