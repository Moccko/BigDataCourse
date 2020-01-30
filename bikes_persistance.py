from requests import post, put
import os
import sys
import json
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
endpoint_url = os.getenv("ENDPOINT_URL")
authorization = os.getenv("AUTHENTICATION_TOKEN")


def send_data(method: callable, url: str, data: str):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authorization
    }

    response = method(url, headers=headers, data=data)
    response.raise_for_status()


def init_index():
    print("Creating the ElasticSearch index with the right mapping")
    mappings = {
        "mappings": {
            "properties": {
                "Timestamp": {
                    "type": "date"
                },
                "location": {
                    "type": "geo_point"
                }
            }
        }
    }
    send_data(put, f"{endpoint_url}/stations", json.dumps(mappings))


def send_all_files(batchsize: int):
    for dirname, _, filenames in tqdm(os.walk("data/ndjson")):
        for filename in filenames:
            # if filename.split('.')[-1] == "ndjson":
            with open(os.path.join(dirname, filename), "r") as f:
                data = f.readlines()
                print(data)
                for i in range(0, len(data), batchsize):
                    batch = ''.join(data[i:i + batchsize])
                    send_data(post, f"{endpoint_url}/_bulk", batch + "\n")


if __name__ == "__main__":
    init_index()
    send_all_files(int(sys.argv[1]) if len(sys.argv) > 1 else 10000)
