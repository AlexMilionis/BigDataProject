from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("query1_sql_parquet") \
    .getOrCreate()

path = 'hdfs://master:9000/datasets/merged_crime_data/'

df = spark.read.parquet(path)

# temporary view/temporary table that allows to run SQL queries against the DataFrame using Spark's SQL engine
df.createOrReplaceTempView("crime_data")

# Convert the date format
spark.sql("""
    CREATE OR REPLACE TEMP VIEW crime_data_agg AS
    SELECT
        DATE_FORMAT(
            TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a'),
            'yyyy/MM'
        ) AS DATE_OCC
    FROM crime_data
""")

# creates [yyyy/MM, crime_total] table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW crime_data_agg2 AS
    SELECT
        SPLIT(DATE_OCC, '/')[0] AS year,
        SPLIT(DATE_OCC, '/')[1] AS month,
        COUNT(*) AS crime_total
    FROM crime_data_agg
    GROUP BY DATE_OCC
""")

# final query
result = spark.sql("""
    SELECT *
    FROM (
        SELECT
            year,
            month,
            crime_total,
            DENSE_RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
        FROM crime_data_agg2
    ) ranked_data
    WHERE ranking IN (1, 2, 3)
    ORDER BY year ASC, crime_total DESC
""")

result.show()

spark.stop()
