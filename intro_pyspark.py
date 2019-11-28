from pyspark import *
from math import *
import json

sc = SparkContext()

rddStr = sc.textFile('data/worldcitiespop.txt').cache()
print(rddStr.count())
# Cleaning populations
rddStr = rddStr.map(lambda x: x.split(','))
# print(rddStr.count())

header = rddStr.first()
rddStr = rddStr.filter(lambda x: x[4] != '' and x != header)
# print(rddStr.count())
# print(*rddStr.take(30), sep='\n')

test = rddStr.take(10)
print(*test[0], sep="\t")
print(*test[1], sep="\t")


def mkdict(t):
    return {"_doc": dict((zip(["city", "country", "localisation", "dept", "pop"],
                              [t[1], t[0], {"lat": float(t[5]), "lon": float(t[6])}, t[3], float(t[4])])))}


clean_cities = rddStr.map(mkdict).collect()

with open("data/clean_cities.json", "w") as fw:
    for city in clean_cities:
        fw.write('{"index": {"_index": "anavis"}}\n')
        fw.write(json.dumps(city) + "\n")

# for t in test[1:]:
#     print(dict((zip(["city", "country", "localisation", "dept", "pop"],
#                     [t[1], t[0], {"lat": t[5], "lon": t[6]}, t[3], t[4]]))))

# {
#     "city": "Paris",
#     "country": "fr",
#     "localisation: {"lat": 45, "lon": 0 },
#     "dept" : 75,
#     "pop" : 100000
# }

# test.foreach(lambda x: print(x))

# Display stats
# rddStr.stdev()
