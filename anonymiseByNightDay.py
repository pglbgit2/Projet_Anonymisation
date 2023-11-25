import random
import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, udf, first
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas

def generateString():
    size = 7
    autorizedChar = string.ascii_letters + string.digits  
    generatedString = ''.join(random.choice(autorizedChar) for _ in range(size))
    return generatedString

def anonymise(fileToReadName, fileToWriteName):
    tab = CSVManager.readTabCSVFile(fileToReadName)
    idAno = {}
    for value in tab:
        if(value[0] not in idAno):
            pseudo = generateString()
            idAno[value[0]] = pseudo
            value[0] = pseudo
        else:
            value[0] = idAno[value[0]]
    CSVManager.writeTabCSVFile(tab,fileToWriteName)


spark = SparkSession.builder.appName("CalculatingDayNightPosition").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")
df = spark.read.csv("../default.csv", header=False, schema=schema, sep='\t')
print(">after reading original file")


dfDay = df.filter((hour(df["timestamp"]) >= 6) & (hour(df["timestamp"]) < 22))
dfNight = df.filter((hour(df["timestamp"]) < 6) | (hour(df["timestamp"]) >= 22))
print(">after day and night dataframe")

avg_jour = dfDay.groupBy("id","timestamp").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
avg_nuit = dfNight.groupBy("id","timestamp").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
print(">after average location")

variation = 0.0001
values_jour = dfDay.join(avg_jour, ["id", "timestamp"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
values_nuit = dfNight.join(avg_nuit, ["id", "timestamp"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
print(">after generating new values")

almost_ready_to_get_anonymized = values_jour.union(values_nuit)
almost_ready_to_get_anonymized = almost_ready_to_get_anonymized.withColumn("longitude", col("new_longitude"))
almost_ready_to_get_anonymized = almost_ready_to_get_anonymized.withColumn("latitude", col("new_latitude"))
print(">after union")


ready_to_be_anonymised = almost_ready_to_get_anonymized.groupBy("id","timestamp","longitude","latitude").agg(weekofyear("timestamp").alias("week"))
print(">renaming columns")


ready_to_be_anonymised = ready_to_be_anonymised.drop("week")
print(">after droping week")
CSVManager.writeTabCSVFile(ready_to_be_anonymised.toPandas(), "NOTANONYM.csv")
anonymise("NOTANONYM.csv", "anonymFrangipane.csv")