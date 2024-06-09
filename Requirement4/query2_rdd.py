from pyspark.sql import SparkSession
from pyspark.sql import Row
import csv

sc = SparkSession \
    .builder \
    .appName("query2_rdd") \
    .getOrCreate() \
    .sparkContext

path = 'hdfs://master:9000/datasets/'

def parse_csv(line):
    return next(csv.reader([line]))

def convert_part_of_day(line):
        if 500 <= int(line[3]) <= 1159:
                return "Morning"
        elif 1200 <= int(line[3]) <= 1659:
                return "Afternoon"
        elif 1700 <= int(line[3]) <= 2059:
                return "Evening"
        else:
                return "Night"

a = sc.textFile(path+"crime_data.csv") \
        .map(parse_csv)

b = sc.textFile(path+"crime_data2.csv") \
        .map(parse_csv)

rdd = a.union(b)
rdd = rdd.filter(lambda x: x[14] == '101') \
        .map(convert_part_of_day) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y: x+y) \
        .sortBy(lambda x: x[1], ascending=False)

print(rdd.collect())

sc.stop()
