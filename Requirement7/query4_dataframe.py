from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, count, avg
from pyspark.sql.types import StringType,DoubleType
from geopy import distance

spark = SparkSession.builder \
    .appName("query4_dataframe") \
    .getOrCreate()

path = 'hdfs://master:9000/datasets/'

df1 = spark.read.csv(path + "crime_data.csv", header=True)
df2 = spark.read.csv(path + "crime_data2.csv", header=True)
police_stations = spark.read.csv(path + "police_stations.csv", header=True)
crime_data = df1.union(df2)

crime_data = crime_data.select(col('Weapon Used Cd'), col('AREA'), col('LAT'), col('LON'))
strip_leading_zeros_udf = udf(lambda x: x.lstrip('0'), StringType())
crime_data = crime_data.withColumn('AREA', strip_leading_zeros_udf(col('AREA')))
police_stations = police_stations.select(col('X'), col('Y'), col('PREC'), col('DIVISION'))

# keep weapons 1xx
crime_data = crime_data.filter(col('Weapon Used Cd').startswith('1'))

# filter out Null Island
crime_data = crime_data.filter((col('LAT') != 0) & (col('LON') != 0))

police_stations = police_stations.withColumnRenamed('PREC', 'AREA')
crime_data = crime_data.join(police_stations, on=['AREA'], how='inner')

def get_distance(lat1, long1, lat2, long2):
    return distance.geodesic((lat1, long1), (lat2, long2)).km

get_distance_udf = udf(lambda lat1, long1, lat2, long2: get_distance(lat1, long1, lat2, long2), DoubleType())

crime_data = crime_data.withColumn("distance", get_distance_udf(crime_data["LAT"], crime_data["LON"], crime_data["Y"], crime_data["X"]))

crime_data = crime_data.select(col('DIVISION'), col('distance'))

crime_data = crime_data.withColumnRenamed('DIVISION', 'division')

crime_data = crime_data.groupBy("DIVISION").agg(avg("distance").alias("average_distance"), count("*").alias("incidents total"))

crime_data.orderBy("incidents total", ascending=False).show(30)

spark.stop()
