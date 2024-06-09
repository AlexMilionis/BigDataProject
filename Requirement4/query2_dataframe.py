from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("query2_dataframe") \
    .getOrCreate()

path = 'hdfs://master:9000/datasets/'
# Load the CSV file into a Spark DataFrame

df1 = spark.read.csv(path+"crime_data.csv", header=True)
df2 = spark.read.csv(path+"crime_data2.csv", header=True)

# merge the two dataframes
df = df1.union(df2)

df = df.filter(col('Premis Cd') == '101') \
        .select('TIME OCC') \
        .withColumn(
                "TIME OCC",
                when((col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159), "Morning")
                .when((col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659), "Afternoon")
                .when((col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059), "Evening")
                .otherwise("Night")
        ) \
        .groupBy("TIME OCC") \
        .count() \
        .orderBy(col("count").desc())

df.show()

spark.stop()
