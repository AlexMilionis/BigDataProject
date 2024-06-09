from pyspark.sql import SparkSession
from geopy import distance
import csv
from io import StringIO

sc = SparkSession.builder \
        .appName("query4_rdd_repartition") \
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

# create rdd's with tag 'crime' or police_station' using tuples
crime_data_tagged = crime_data.map(lambda x: (x[0], ('crime',x[1],x[2],x[3]) ))
police_stations_tagged = police_stations.map(lambda x: (x[0], ('police_station',x[1],x[2],x[3]) ))
# merge the 2 rdd's for implementation purposes
union_rdd = crime_data_tagged.union(police_stations_tagged)

# Perform the join operation on records of the same partition
def combine_records(dataset_tagged):
    key = dataset_tagged[0]
    val = dataset_tagged[1]

    crime_list, police_stations_list = [], []
    for v in val:
        if v[0] == 'crime':
            crime_list.append(v[1:])
        elif v[0] == 'police_station':
            police_stations_list.append(v[1:])
    to_return = []
    for cr in crime_list:
        for ps in police_stations_list:
            to_return.append((key,) + cr + ps)
    return to_return

# Group records with same key in the same partition, then join them
joined_rdd = union_rdd.groupByKey() \
             .flatMap(combine_records)

def add_distance_column(row):
    lat1, long1 = float(row[2]), float(row[3])
    lat2, long2 = float(row[5]), float(row[4])
    dist = distance.geodesic((lat1, long1), (lat2, long2)).km
    return row + (dist,)

joined_rdd = joined_rdd.map(add_distance_column)

joined_rdd = joined_rdd.map(lambda x: (x[-2], x[-1]))
joined_rdd = joined_rdd.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: (x[0] / x[1], x[1])) \
    .map(lambda x: (x[0], x[1][0], x[1][1])) \
    .sortBy(lambda x: x[2], ascending=False)

print(joined_rdd.collect())

sc.stop()
