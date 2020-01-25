import requests
import os
import time

endpoint_url = "https://eb8e3c6481d4463fafe4833aec3119d8.eu-central-1.aws.cloud.es.io:9243"
authorization = "Basic ZWxhc3RpYzpUM1FaT1VMellmdHdWSktFamxlQ05yenk="


def send_all_files():
    for dirname, _, filenames in os.walk("data/ndjson01"):
        for filename in filenames:
            if filename.split('.')[-1] == "ndjson":
                with open(os.path.join(dirname, filename), "r") as f:
                    data = f.readlines()
                    step = 10000
                    for i in range(0, len(data), step):
                        batch = ''.join(data[i:i + step])
                        print(batch, sep="")
                        # print(len(data[i:i + step]))
                        send_data(batch + "\n")


def send_data(data):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authorization
    }

    response = requests.post(f"{endpoint_url}/_bulk", headers=headers, data=data)
    response.raise_for_status()

# for dirname, _, filenames in os.walk("data/ndjson"):
#     for filename in filenames:
#         if filename.split('.')[-1] == "ndjson":
#             with open(os.path.join(dirname, filename), "r") as f:
#                 send_data(f.read())
#                 print("hello ...")
#                 input()
#                 # time.sleep(60)
