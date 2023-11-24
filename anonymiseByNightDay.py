import random
import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, udf, first
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas

def generateString():
    size = 5
    autorizedChar = string.ascii_letters + string.digits  
    generatedString = ''.join(random.choice(autorizedChar) for _ in range(size))
    return generatedString

spark = SparkSession.builder.appName("CalculatingDayNightPosition").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")
df = spark.read.csv("tableau.csv", header=False, schema=schema, sep=',')
print(">after reading original file")


dfDay = df.filter((hour(df["timestamp"]) >= 6) & (hour(df["timestamp"]) < 18))
dfNight = df.filter((hour(df["timestamp"]) < 6) | (hour(df["timestamp"]) >= 18))
print(">after day and night dataframe")

avg_jour = dfDay.groupBy("id","timestamp").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
avg_nuit = dfNight.groupBy("id","timestamp").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
print(">after average location")

variation = 0.001
values_jour = dfDay.join(avg_jour, ["id", "timestamp"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
values_nuit = dfNight.join(avg_nuit, ["id", "timestamp"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
print(">after generating new values")

almost_ready_to_get_anonymized = values_jour.union(values_nuit)
almost_ready_to_get_anonymized = almost_ready_to_get_anonymized.withColumn("longitude", col("new_longitude"))
almost_ready_to_get_anonymized = almost_ready_to_get_anonymized.withColumn("latitude", col("new_latitude"))
print(">after union")

ready_to_be_anonymised = almost_ready_to_get_anonymized.groupBy("id","timestamp","longitude","latitude").agg(weekofyear("timestamp").alias("week"))
print(">renaming columns")

generate_id = udf(lambda: generateString(), StringType())
print(">saving generating function into pyspark")

anonymized_tab = ready_to_be_anonymised.groupBy("week").agg(generate_id().alias("identifiant"), first("timestamp").alias("timestamp"),first("longitude").alias("longitude"),first("latitude").alias("latitude"))
anonymized_tab = anonymized_tab.drop("week","id")
print(">Anonymized, writing...")
CSVManager.writeTabCSVFile(anonymized_tab.toPandas(), "anonymFrangipane.csv")