import subprocess
import os
import pandas as pd
import gzip

import json

import requests as http
from tqdm.autonotebook import tqdm

# res = subprocess.check_output(["./init.sh"])
# for line in res.splitlines():
#     print(line)


for dirname, _, filenames in os.walk('data/clean', topdown=True):
    df = pd.DataFrame(columns=['Timestamp', 'Station', 'Bikes', 'Slots', 'Total', 'Status', 'Humidity',
                               'Pressure', 'Rain', 'WindDeg', 'WindSpeed', 'Snow', 'TemperatureTemp'])
    for filename in tqdm(filenames):
        file = os.path.join(dirname, filename)
        with gzip.open(file) as f:
            df = pd.concat([df, pd.read_csv(f, sep=";")])
    if not df.empty:
        df.to_pickle(f"./data/pkl/{dirname.split('/')[-1]}.pkl")


def write_json():
    for dirname, _, filenames in os.walk('data/pkl', topdown=True):
        for filename in sorted(filenames):
            if os.path.exists(os.path.join(dirname, filename)):
                print(filename)
                df = pd.read_pickle(os.path.join(dirname, filename))
                with(open(f"data/ndjson/{filename.split('.')[1]}.ndjson", "w")) as ndjson:
                    for index, row in df.iterrows():
                        ndjson.write('{"index": {"_index": "stations"}}\n')
                        ndjson.write(row.to_json() + '\n')
