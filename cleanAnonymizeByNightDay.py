import random
import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, udf, first
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas

# def generateString():
#     size = 7
#     autorizedChar = string.ascii_letters + string.digits  
#     generatedString = ''.join(random.choice(autorizedChar) for _ in range(size))
#     return generatedString


def readfile(path: str):
    spark = SparkSession.builder.appName("CalculatingDayNightPosition")\
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True)
    ])
    print(">after schema definition")
    return spark.read.csv(path, header=False, schema=schema, sep='\t')

def anonymize_but_not_completely(startOfTheDay, startOfTheNight, df, variation, path):
    dfDay = df.filter((hour(df["timestamp"]) >= startOfTheDay) & (hour(df["timestamp"]) < startOfTheNight))
    dfNight = df.filter((hour(df["timestamp"]) < startOfTheDay) | (hour(df["timestamp"]) >= startOfTheNight))
    print(">after day and night dataframe")

    avg_jour = dfDay.groupBy("id","timestamp").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
    avg_nuit = dfNight.groupBy("id","timestamp").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
    print(">after average location")

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
    # CSVManager.writeTabCSVFile(ready_to_be_anonymised.toPandas(), path)
    ready_to_be_anonymised.coalesce(1).write.csv(path, header=False,  mode="overwrite", sep="\t")

# def pseudonymize(fileToReadName, fileToWriteName):
#     tab = CSVManager.readTabCSVFile(fileToReadName)
#     idAno = {}
#     for value in tab:
#         if(value[0] not in idAno):
#             pseudo = generateString()
#             idAno[value[0]] = pseudo
#             value[0] = pseudo
#         else:
#             value[0] = idAno[value[0]]
#     CSVManager.writeTabCSVFile(tab,fileToWriteName)
    

if __name__=='__main__':
    df = readfile("ReferenceINSA.csv")
    print(">after reading original file")
    anonymize_but_not_completely(6, 22, df, 0.0001, "nopseudo.csv")
    # pseudonymize("nopseudo.csv", "anonymFrangipane.csv")