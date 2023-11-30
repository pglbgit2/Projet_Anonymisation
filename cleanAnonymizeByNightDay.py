import random
import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, monotonically_increasing_id, expr, row_number
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
        StructField("timestamp", TimestampType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True)
    ])
    print(">after schema definition")
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id())
    

def anonymize_but_not_completely(startOfTheDay, startOfTheNight, df, variation, path):
    dfDay = df.filter((hour(df["timestamp"]) >= startOfTheDay) & (hour(df["timestamp"]) < startOfTheNight))
    dfNight = df.filter((hour(df["timestamp"]) < startOfTheDay) | (hour(df["timestamp"]) >= startOfTheNight))
    print(">after day and night dataframe")

    dfDay = dfDay.withColumn("week", weekofyear("timestamp"))
    dfNight = dfNight.withColumn("week", weekofyear("timestamp"))
    
    print(">adding week")


    avg_jour = dfDay.groupBy("id","week").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
    avg_nuit = dfNight.groupBy("id","week").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))
    # avg_jour.coalesce(1).write.csv("test_jour", header=True,  mode="overwrite", sep="\t")
    # avg_jour.coalesce(1).write.csv("test_nuit", header=True,  mode="overwrite", sep="\t")

    print(">after average location")

    values_jour = dfDay.join(avg_jour, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude")
    values_nuit = dfNight.join(avg_nuit, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude")

    values_jour.show()

    # values_jour = dfDay.join(avg_jour, (dfDay.id == avg_jour.id) & (dfDay.week == avg_jour.week)).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
    # values_nuit = dfNight.join(avg_nuit, (dfNight.id == avg_nuit.id) & (dfNight.week == avg_nuit.week)).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
    print(">after generating new values")

    ready_to_be_anonymised = values_jour.union(values_nuit)
    # ready_to_be_anonymised = ready_to_be_anonymised.withColumn("longitude", col("new_longitude"))
    # ready_to_be_anonymised = ready_to_be_anonymised.withColumn("latitude", col("new_latitude"))
    print(">after union")

 
    # CSVManager.writeTabCSVFile(ready_to_be_anonymised.toPandas(), path)
    final = ready_to_be_anonymised.withColumn("timestamp", expr("substring(timestamp, 1, length(timestamp)-3) || ':00'"))
    final_sorted = final.sort("numero_ligne", ascending=[True])
    final_sorted.coalesce(1).write.csv(path, header=True,  mode="overwrite", sep="\t")

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
    df.show()
    print(">after reading original file")
    anonymize_but_not_completely(6, 22, df, 0.0001, "nopseudo.csv")
    # pseudonymize("nopseudo.csv", "anonymFrangipane.csv")