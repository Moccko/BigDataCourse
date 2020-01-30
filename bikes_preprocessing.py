import os
import pandas as pd
import numpy as np
from tqdm.autonotebook import tqdm

df_positions = pd.read_csv("data/brut/bicincitta_parma_summary.csv", header=0, sep=';')

columns = ['Timestamp', 'Station', 'Bikes', 'Slots', 'Total', 'Status', 'Humidity',
           'Pressure', 'Rain', 'WindDeg', 'WindSpeed', 'Snow', 'TemperatureTemp']


def write_pkl():
    for dirname, _, files in os.walk("data/clean", topdown=True):
        name = dirname.split('/')[-1]
        if name == "clean":
            continue  # exclude top level directory

        if os.path.exists(f"data/pkl/{name}.pkl"):
            print(f"Already pickled:\tstation {name}")
        else:
            # Location extraction
            station = dirname.split('/')[-1]
            if station == "25. Ospedale":
                station = "02. Ospedale Maggiore"

            station_position = df_positions[df_positions["station"] == station][["latitude", "longitude"]].values[0]
            station_position = {"lat": station_position[0], "lon": station_position[1]}

            # Dataframes concatenation
            df = pd.concat([pd.read_csv(os.path.join(dirname, f), compression="gzip", sep=";") for f in tqdm(files)])

            # Timestamp preprocessing, location and day insertion
            df["location"] = np.repeat(station_position, len(df))
            df.Status = df.Status.apply(lambda x: x.lower())
            df.Timestamp = df.Timestamp.apply(lambda d: pd.Timestamp(d))
            df["day"] = df.Timestamp.dt.dayofweek

            # Remove absolute zeroes
            print(f"Removed {len(df[df['TemperatureTemp'] <= -273.15])} absolute zeroes")
            df = df[df["TemperatureTemp"] > -273.15]

            # Pickle persistance in case of future processes
            os.makedirs("data/pkl", exist_ok=True)
            df.to_pickle(f"data/pkl/{name}.pkl")
            print(f"Pickled:\t\t\tstation {name}")


def write_ndjson():
    os.makedirs("data/ndjson", exist_ok=True)
    for dirname, _, filenames in os.walk("data/pkl", topdown=True):
        for filename in tqdm(filenames):
            df = pd.read_pickle(os.path.join(dirname, filename))
            with(open(f"data/ndjson/{filename.split('.')[1]}.ndjson", "w")) as ndjson:
                for index, row in df.iterrows():
                    ndjson.write('{"index": {"_index": "stations"}}\n')
                    ndjson.write(row.to_json() + '\n')


if __name__ == "__main__":
    write_pkl()
    write_ndjson()
