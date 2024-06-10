from pyspark.sql           import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, regexp_replace, count

spark = SparkSession.builder \
    .appName("query3_merge") \
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

# Sorting datasets on join keys before the join
df = df.orderBy(['LAT', 'LON'])
zips = zips.orderBy(['LAT', 'LON'])

print('===========================')
df_zips = df.hint("merge").join(zips.hint("merge"), on=['LAT', 'LON'], how='inner')
print("Explain for first join:")
df_zips.explain("simple")
print('===========================')

# rename so that the zip column is the same
df_zips = df_zips.withColumnRenamed('ZIPcode', 'Zip Code')

# Sorting datasets on join keys before the second join
df_zips = df_zips.orderBy('Zip Code')
incomes = incomes.orderBy('Zip Code')

df_zips_income = df_zips.hint("merge").join(incomes.hint("merge"), on=['Zip Code'], how='inner')
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

top_3_zips    = [row['Zip Code'] for row in top_3.collect()]
bottom_3_zips = [row['Zip Code'] for row in bottom_3.collect()]

top_3    = df_zips_income.filter(col('Zip Code').isin(top_3_zips))
bottom_3 = df_zips_income.filter(col('Zip Code').isin(bottom_3_zips))

top_3 = top_3.groupBy('victim descent').agg(count('*').alias('total victims'))
top_3 = top_3.orderBy(col('total victims').desc())

bottom_3 = bottom_3.groupBy('victim descent').agg(count('*').alias('total victims'))
bottom_3 = bottom_3.orderBy(col('total victims').desc())

top_3.show()
bottom_3.show()



spark.stop()
