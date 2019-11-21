from pyspark import *
from math import *

sc = SparkContext()

rddStr = sc.textFile('data/worldcitiespop.txt').cache()
print(rddStr.count())
# Cleaning populations
rddStr = rddStr.map(lambda x: x.split(','))
print(rddStr.count())
rddStr = rddStr.filter(lambda x: x[4] != '')
print(rddStr.count())
# print(*rddStr.take(30), sep='\n')

# Display stats
rddStr.stdev()
