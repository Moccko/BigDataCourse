#!/usr/bin/env python
# coding: utf-8

from pyspark import *
from pyspark.mllib.stat import Statistics

from math import *
import json

import requests as http
from tqdm.autonotebook import tqdm

sc = SparkContext()

rddStr = sc.textFile("data/worldcitiespop.txt").cache()

# Cleaning populations and applying proper types

rddStr = rddStr.map(lambda x: x.split(","))
header = rddStr.first()
rddStr = rddStr.filter(lambda x: x[4] != "" and x != header)
rddStr = rddStr.map(
    lambda x: [x[0], x[1], x[2], x[3], int(x[4]), float(x[5]), float(x[6])]
)


# Making populations as dict for ElasticSearch serialization

def mk_pop_dict(t):
    return {
        "_doc": dict(
            (
                zip(
                    ["city", "country", "localisation", "dept", "pop"],
                    [t[1], t[0], {"lat": t[5], "lon": t[6]}, t[3], t[4]],
                )
            )
        )
    }


clean_cities = rddStr.map(mk_pop_dict).collect()

# Writing in the ndjson file

with open("data/clean_cities.json", "w") as fw:
    for city in clean_cities:
        fw.write('{"index": {"_index": "worldcitiespop"}}\n')
        fw.write(json.dumps(city) + "\n")

# Display stats
summary = Statistics.colStats(rddStr)
summary.stddev()

adultRdd = sc.textFile("data/adult.data").cache()

adultRdd = adultRdd.map(lambda x: x.split(", "))


def convert_columns(x):
    ints = [0, 2, 4, 10, 11, 12]
    for i in ints:
        try:
            x[i] = int(x[i])
        except:
            print(x)
    return x


adultRdd = adultRdd.map(convert_columns)


def persist(adult):
    adult = dict(
        zip(
            [
                "age",
                "workclass",
                "fnlwgt",
                "education",
                "education-num",
                "marital-status",
                "occupation",
                "relationship",
                "race",
                "sex",
                "capital-gain",
                "capital-loss",
                "hours-per-week",
                "native-country",
                "yearly-wage",
            ],
            adult,
        )
    )
    print(http.post("http://localhost:9200/adults/_doc", json=adult).status_code)
    return adult


adults = adultRdd.map(persist).collect()

with open("data/clean_adults.json", "w") as fw:
    for adult in tqdm(adults):
        fw.write('{"index": {"_index": "adults"}}\n')
        fw.write(json.dumps(adult) + "\n")
