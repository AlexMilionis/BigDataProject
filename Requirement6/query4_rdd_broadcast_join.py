from pyspark.sql import SparkSession
from geopy import distance
import csv
from io import StringIO

sc = SparkSession.builder \
        .appName("query4_rdd_broadcast_join") \
        .getOrCreate() \
        .sparkContext

path = 'hdfs://master:9000/datasets/'

def parse_csv(line):
    if not line.strip():  # Skip empty lines
        return None
    sio = StringIO(line)
    reader = csv.reader(sio)
    return next(reader)

# read the necessary csv files
rdd1 = sc.textFile(path+"crime_data.csv")
header_rdd1 = rdd1.first()
rdd1 = rdd1.filter(lambda line: line != header_rdd1) \
        .map(parse_csv) \
        .filter(lambda x: x is not None)

rdd2 = sc.textFile(path+"crime_data2.csv")
header_rdd2 = rdd2.first()
rdd2 = rdd2.filter(lambda line: line != header_rdd2) \
        .map(parse_csv) \
        .filter(lambda x: x is not None)
crime_data = rdd1.union(rdd2)

police_stations = sc.textFile(path+"police_stations.csv")
header_police_stations = police_stations.first()
police_stations = police_stations.filter(lambda line: line != header_police_stations) \
        .map(parse_csv) \
        .filter(lambda x: x is not None)

crime_data = crime_data.map(lambda x:[x[i] for i in [16, 4, 26, 27]])
police_stations = police_stations.map(lambda x:[x[i] for i in [5, 0, 1, 3]])

# keep weapons 1xxx
crime_data = crime_data.filter(lambda row: row[0] is not None and row[0].startswith('1'))

crime_data = crime_data.map(lambda x: [x[1].lstrip('0'), x[0], x[2], x[3]])

# filter out Null Island
crime_data = crime_data.filter(lambda row: (row[2] != '0') & (row[3] != '0'))

# Broadcast the smaller RDD (police stations)
small_dataset = police_stations.keyBy(lambda t: int(t[0]))
broadcast_small = sc.broadcast(small_dataset.collectAsMap())
large_dataset = crime_data.filter(lambda x: x[0] is not None).keyBy(lambda t: int(t[0]))


def combine_records(row_large_dataset):
    key = row_large_dataset[0]
    matching_value = broadcast_small.value.get(key, None)
    if matching_value:
        return row_large_dataset[1] + matching_value[1:]
    else:
        return None

# Apply the join and filter out None results
joined_rdd = large_dataset.map(combine_records).filter(lambda x: x is not None)

def add_distance_column(row):
    lat1, long1 = float(row[2]), float(row[3])
    lat2, long2 = float(row[5]), float(row[4])
    dist = distance.geodesic((lat1, long1), (lat2, long2)).km
    return row + [dist,]

joined_rdd = joined_rdd.map(add_distance_column)

joined_rdd = joined_rdd.map(lambda x: [x[-2], x[-1]])
joined_rdd = joined_rdd.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: (x[0] / x[1], x[1])) \
    .map(lambda x: (x[0], x[1][0], x[1][1])) \
    .sortBy(lambda x: x[2], ascending=False)

print(joined_rdd.collect())

sc.stop()
