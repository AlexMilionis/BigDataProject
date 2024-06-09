from pyspark.sql           import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, regexp_replace, count

spark = SparkSession.builder \
    .appName("query3_shuffle_replicate_nl") \
    .getOrCreate()

path = 'hdfs://master:9000/datasets/'

# since we only need for 2015, we read the first csv
df      = spark.read.csv(path + "crime_data.csv",     header=True, inferSchema=True)
zips    = spark.read.csv(path + "revgecoding.csv",    header=True, inferSchema=True)
incomes = spark.read.csv(path + "LA_income_2015.csv", header=True, inferSchema=True)

# keep necessary columns
df = df.select(col('DATE OCC'), col('Vict Descent'), col('LAT'), col('LON'))

# modify and keep only 2015
df = df.withColumn('DATE OCC', to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn('DATE OCC', date_format(col('DATE OCC'), 'yyyy'))
df = df.filter(col('DATE OCC') == '2015')

# Remove Null
df = df.na.drop(subset=['Vict Descent'])

# we keep the first zip for the same lat, lon
zips = zips.dropDuplicates(['LAT', 'LON'])

print('===========================')
df_zips = df.hint("shuffle_replicate_nl").join(zips.hint("shuffle_replicate_nl"), on=['LAT', 'LON'], how='inner')
print("Explain for first join:")
df_zips.explain("simple")
print('===========================')

# rename so that the zip column is the same
df_zips = df_zips.withColumnRenamed('ZIPcode', 'Zip Code')

df_zips_income = df_zips.hint("shuffle_replicate_nl").join(incomes.hint("shuffle_replicate_nl"), on=['Zip Code'], how='inner')
print('===========================')
print("Explain for second join:")
df_zips_income.explain("simple")
print('===========================')

# no need for lat, lon, date, community
df_zips_income = df_zips_income.select(col('Zip Code'), col('Vict Descent'), col('Estimated Median Income'))

# convert income to integer
df_zips_income = df_zips_income.withColumn('Estimated Median Income',
                                           regexp_replace(col('Estimated Median Income'), ',', ''))
df_zips_income = df_zips_income.withColumn('Estimated Median Income', col('Estimated Median Income').substr(2, 100))
df_zips_income = df_zips_income.withColumn('Estimated Median Income', col('Estimated Median Income').cast('int'))

# find the top 3 and bottom 3 zip codes
distinct_zip_income = df_zips_income.select("Zip Code", "Estimated Median Income").distinct()

top_3    = distinct_zip_income.orderBy(col("Estimated Median Income").desc()).limit(3)
bottom_3 = distinct_zip_income.orderBy(col("Estimated Median Income").asc()).limit(3)

df_zips_income = df_zips_income.select(col('Zip Code'), col('Vict Descent'))
df_zips_income = df_zips_income.withColumnRenamed('Vict Descent', 'victim descent')

top_3          = top_3.select(col('Zip Code'))
bottom_3       = bottom_3.select(col('Zip Code'))

top_3    = df_zips_income.join(top_3, "Zip Code", 'inner')
bottom_3 = df_zips_income.join(bottom_3, "Zip Code", 'inner')

top_3 = top_3.groupBy('victim descent').agg(count('*').alias('total victims'))
top_3 = top_3.orderBy(col('total victims').desc())

bottom_3 = bottom_3.groupBy('victim descent').agg(count('*').alias('total victims'))
bottom_3 = bottom_3.orderBy(col('total victims').desc())

top_3.show()
bottom_3.show()



spark.stop()
