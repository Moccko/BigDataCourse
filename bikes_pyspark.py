import subprocess
import os
import pandas as pd
import gzip
import numpy as np

import json

import requests as http
from tqdm.autonotebook import tqdm

# res = subprocess.check_output(["./init.sh"])
# for line in res.splitlines():
#     print(line)
df_positions = pd.read_csv('data/bicincitta_parma_summary.csv', header=0, sep=';')

for dirname, _, filenames in os.walk('data/clean', topdown=True):
    df = pd.DataFrame(columns=['Timestamp', 'Station', 'Bikes', 'Slots', 'Total', 'Status', 'Humidity',
                               'Pressure', 'Rain', 'WindDeg', 'WindSpeed', 'Snow', 'TemperatureTemp'])

    for filename in tqdm(filenames):
        file = os.path.join(dirname, filename)
        with gzip.open(file) as f:
            df = pd.concat([df, pd.read_csv(f, sep=";")])
    if not df.empty:
        # print(dirname.split('data/clean/')[1])
        fil = dirname.split('data/clean/')[1]
        # print(df_positions[df_positions['station'] == fil].index[0])
        if fil == "25. Ospedale":
            row_index = df_positions[df_positions['station'] == '02. Ospedale Maggiore'].index[0]
        else:
            row_index = df_positions[df_positions['station'] == fil].index[0]

        station_position = {"lat": df_positions['latitude'][row_index], "lon": df_positions['longitude'][row_index]}
        station_position = str(station_position)
        tmp = np.repeat(station_position, len(df))
        df['location'] = tmp
        print(df.location)
        df['Timestamp'] = df['Timestamp'].apply(lambda d: pd.Timestamp(d))
        df['day'] = df['Timestamp'].dt.dayofweek
        print(df.columns)
        df.to_pickle(f"./data/pkl/{dirname.split('/')[-1]}.pkl")


def write_json():
    for dirname, _, filenames in os.walk('data/pkl_clean', topdown=True):
        for filename in sorted(filenames):
            if os.path.exists(os.path.join(dirname, filename)):
                print(filename)
                df = pd.read_pickle(os.path.join(dirname, filename))
                with(open(f"data/ndjson_clean/{filename.split('.')[1]}.ndjson", "w")) as ndjson:
                    for index, row in df.iterrows():
                        ndjson.write('{"index": {"_index": "stations"}}\n')
                        ndjson.write(row.to_json() + '\n')


def write_pkl():
    for dirname, _, filenames in os.walk('data/pkl01', topdown=True):
        for filename in sorted(filenames):
            if os.path.exists(os.path.join(dirname, filename)):
                print(filename)
                df = pd.read_pickle(os.path.join(dirname, filename))
                df.location = df.location.apply(lambda x: str(x))
                df.Timestamp = df.Timestamp.apply(lambda x: x.date())
                df.to_pickle(f"data/pkl_clean/{filename}")


write_json()

for dirname, _, filenames in os.walk('data/pkl01', topdown=True):
    df = pd.DataFrame(columns=['Timestamp', 'Station', 'Bikes', 'Slots', 'Total', 'Status', 'Humidity',
                               'Pressure', 'Rain', 'WindDeg', 'WindSpeed', 'Snow', 'TemperatureTemp'])

    for filename in tqdm(filenames):
        file = os.path.join(dirname, filename)
        if not df.empty:
            # print(dirname.split('data/clean/')[1])
            fil = dirname.split('data/clean/')[1]
            # print(df_positions[df_positions['station'] == fil].index[0])
            if fil == "25. Ospedale":
                row_index = df_positions[df_positions['station'] == '02. Ospedale Maggiore'].index[0]
            else:
                row_index = df_positions[df_positions['station'] == fil].index[0]

            station_position = {"lat": df_positions['latitude'][row_index], "lon": df_positions['longitude'][row_index]}
            station_position = str(station_position)
            tmp = np.repeat(station_position, len(df))
            df['location'] = tmp
            print(df.location)
            df['Timestamp'] = df['Timestamp'].apply(lambda d: pd.Timestamp(d))
            df['day'] = df['Timestamp'].dt.dayofweek
            print(df.columns)
            df.to_pickle(f"./data/pkl/{dirname.split('/')[-1]}.pkl")
