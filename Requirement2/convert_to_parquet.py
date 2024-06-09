from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to Parquet Conversion") \
    .getOrCreate()

csv_file_path1 = "hdfs://master:9000/datasets/crime_data.csv"
csv_file_path2 = "hdfs://master:9000/datasets/crime_data2.csv"
parquet_file_path = "hdfs://master:9000/datasets/merged_crime_data"

df1 = spark.read.csv(csv_file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_file_path2, header=True, inferSchema=True)

# Merge datasets
merged_df = df1.union(df2)
merged_df.write.parquet(parquet_file_path)

spark.stop()
