from pyspark import *
from math import *
import json

sc = SparkContext()

rddStr = sc.textFile('data/worldcitiespop.txt').cache()
print(rddStr.count())

# Cleaning populations and applying proper types
rddStr = rddStr.map(lambda x: x.split(','))
header = rddStr.first()
rddStr = rddStr.filter(lambda x: x[4] != '' and x != header)
rddStr = rddStr.map(lambda x: [x[0], x[1], x[2], x[3], int(x[4]), float(x[5]), float(x[6])])


# print(*rddStr.take(30), sep='\n')

def mk_pop_dict(t):
    return {"_doc": dict((zip(["city", "country", "localisation", "dept", "pop"],
                              [t[1], t[0], {"lat": t[5], "lon": t[6]}, t[3], t[4]])))}


clean_cities = rddStr.map(mk_pop_dict).collect()

with open("data/clean_cities.json", "w") as fw:
    for city in clean_cities:
        fw.write('{"index": {"_index": "anavis"}}\n')
        fw.write(json.dumps(city) + "\n")

# Display stats
# rddStr.stats()
