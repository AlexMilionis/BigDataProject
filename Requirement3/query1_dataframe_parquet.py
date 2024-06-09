from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank, col, to_timestamp, when, date_format, count, split
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("query1_dataframe_parquet") \
    .getOrCreate()

path = 'hdfs://master:9000/datasets/merged_crime_data'

df = spark.read.parquet(path)

df = df.select('DATE OCC')

df = df.withColumn('DATE OCC', to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))

df = df.withColumn('DATE OCC', date_format(col('DATE OCC'), 'yyyy/MM'))

df_grouped = df.groupBy('DATE OCC').agg(count('*').alias('crime_total'))

# split year and month into different columns, drop yyyy/MM
df_grouped = df_grouped.withColumn('year', split(col('DATE OCC'), '/').getItem(0))
df_grouped = df_grouped.withColumn('month', split(col('DATE OCC'), '/').getItem(1))
df_grouped = df_grouped.drop('DATE OCC')

# groups data by year to create the ranking column
window_spec = Window.partitionBy('year').orderBy(col('crime_total').desc())
df_grouped = df_grouped.withColumn('ranking', dense_rank().over(window_spec))

# sort by ascending year and descending crime_total
df_grouped = df_grouped.filter(col('ranking').isin(1, 2, 3))
df_grouped = df_grouped.orderBy(col('year').asc(), col('crime_total').desc())

# re-arrange column positions
df_grouped = df_grouped.select('year', 'month', 'crime_total', 'ranking')
df_grouped.show()

spark.stop()
