import random
import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, dayofweek, randn, col, weekofyear, monotonically_increasing_id, expr, udf, array
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import random
import CSVManager
import pandas

def NegativePositiveInteger():
    if random.random() > 0.5:
        return 1
    else:
        return -1
    #Simple as

def generateString():
    size = 7
    autorizedChar = string.ascii_letters + string.digits  
    generatedString = ''.join(random.choice(autorizedChar) for _ in range(size))
    return generatedString


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

    values_jour = dfDay.join(avg_jour, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation*NegativePositiveInteger()).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation*NegativePositiveInteger()).drop("avg_longitude", "avg_latitude")
    values_nuit = dfNight.join(avg_nuit, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation*NegativePositiveInteger()).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation*NegativePositiveInteger()).drop("avg_longitude", "avg_latitude")


    # values_jour = dfDay.join(avg_jour, (dfDay.id == avg_jour.id) & (dfDay.week == avg_jour.week)).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
    # values_nuit = dfNight.join(avg_nuit, (dfNight.id == avg_nuit.id) & (dfNight.week == avg_nuit.week)).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
    print(">after generating new values")

    ready_to_be_anonymised = values_jour.union(values_nuit)
    # ready_to_be_anonymised = ready_to_be_anonymised.withColumn("longitude", col("new_longitude"))
    # ready_to_be_anonymised = ready_to_be_anonymised.withColumn("latitude", col("new_latitude"))
    print(">after union")

 
    # CSVManager.writeTabCSVFile(ready_to_be_anonymised.toPandas(), path)
    almost_finished = ready_to_be_anonymised.withColumn("timestamp", expr("substring(timestamp, 1, length(timestamp)-3) || ':00'"))
    print(">rounded to minute")

    sorted = almost_finished.sort("numero_ligne", ascending=[True])

    idPseudoDic = {}
    generatedStrings = set()
    generatedStrings.add('')
    pseudonymise_udf = udf(lambda z: pseudonymise(z, idPseudoDic, generatedStrings), StringType())
    anonymised = sorted.withColumn('anonymId', pseudonymise_udf(array('id', 'week')))

    print(">after pseudonymisation")

    # final_sorted = anonymised.sort("numero_ligne", ascending=[True])
    anonymised.coalesce(1).write.csv(path, header=True,  mode="overwrite", sep="\t")

def pseudonymise(array, idAno, generatedStrings):
    [id, week] = array
    if(id not in idAno):
        pseudo = generateString()
        idAno[id] = {}
        idAno[id][week] = pseudo
        return pseudo
    else:
        if(week not in idAno[id]):
            pseudo = ''
            while pseudo in generatedStrings:
                pseudo = generateString()
            idAno[id][week] = pseudo
            return pseudo
        else:
            return idAno[id][week]

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

def anonymiseDayNightWeekend(startOfTheDay, startOfTheNight, df, variation, path):
    dfDay = df.filter((1 < (dayofweek(df["timestamp"])) <= 6) & ((hour(df["timestamp"]) >= startOfTheDay) & (hour(df["timestamp"]) < startOfTheNight)))
    dfNight = df.filter((1 < (dayofweek(df["timestamp"])) <= 6) & ((hour(df["timestamp"]) < startOfTheDay) | (hour(df["timestamp"]) >= startOfTheNight)))
    dfWeekend = df.filter(((dayofweek(df["timestamp"])) == 1 | (dayofweek(df["timestamp"])) == 7))
    # dayofweek() -> 1 for sunday through 7 for a saturday
    # Le Weekend est un seul Gros POI.

    print("> After day, night and weekend dataframes")

    dfDay = dfDay.withColumn("week", weekofyear("timestamp"))
    dfNight = dfNight.withColumn("week", weekofyear("timestamp"))
    dfWeekend = dfWeekend.withColumn("week", weekofyear("timestamp"))

    print("> Adding week")

    avg_jour = dfDay.groupBy("id", "week").agg(avg("longitude").alias("avg_longitude"),
                                               avg("latitude").alias("avg_latitude"))
    avg_nuit = dfNight.groupBy("id", "week").agg(avg("longitude").alias("avg_longitude"),
                                                 avg("latitude").alias("avg_latitude"))
    avg_weekend = dfWeekend.groupBy("id", "week").agg(avg("longitude").alias("avg_longitude"),
                                                 avg("latitude").alias("avg_latitude"))
    # avg_jour.coalesce(1).write.csv("test_jour", header=True,  mode="overwrite", sep="\t")
    # avg_jour.coalesce(1).write.csv("test_nuit", header=True,  mode="overwrite", sep="\t")

    print("> After average location")

    values_jour = dfDay.join(avg_jour, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(
        seed=23) * variation * NegativePositiveInteger()).withColumn("new_latitude", col("avg_latitude") + randn(seed=42) * variation*NegativePositiveInteger()).drop(
        "avg_longitude", "avg_latitude")
    values_nuit = dfNight.join(avg_nuit, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(
        seed=34) * variation * NegativePositiveInteger()).withColumn("new_latitude", col("avg_latitude") + randn(seed=42) * variation*NegativePositiveInteger()).drop(
        "avg_longitude", "avg_latitude")
    values_weekend = dfWeekend.join(avg_weekend, ["id", "week"]).withColumn("new_longitude", col("avg_longitude") + randn(
        seed=26) * variation * NegativePositiveInteger()).withColumn("new_latitude", col("avg_latitude") + randn(seed=42) * variation*NegativePositiveInteger()).drop(
        "avg_longitude", "avg_latitude")

    values_jour.show()

    # values_jour = dfDay.join(avg_jour, (dfDay.id == avg_jour.id) & (dfDay.week == avg_jour.week)).withColumn("new_longitude", col("avg_longitude") + randn(seed=23)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
    # values_nuit = dfNight.join(avg_nuit, (dfNight.id == avg_nuit.id) & (dfNight.week == avg_nuit.week)).withColumn("new_longitude", col("avg_longitude") + randn(seed=34)*variation).withColumn("new_latitude", col("avg_latitude") + randn(seed=42)*variation).drop("avg_longitude", "avg_latitude", "longitude", "latitude")
    print(">after generating new values")

    ready_to_be_anonymised = values_jour.union(values_nuit)
    ready_to_be_anonymised = ready_to_be_anonymised.union(values_weekend)
    # ready_to_be_anonymised = ready_to_be_anonymised.withColumn("longitude", col("new_longitude"))
    # ready_to_be_anonymised = ready_to_be_anonymised.withColumn("latitude", col("new_latitude"))
    print(">after union")

    # CSVManager.writeTabCSVFile(ready_to_be_anonymised.toPandas(), path)
    almost_finished = ready_to_be_anonymised.withColumn("timestamp",
                                                        expr("substring(timestamp, 1, length(timestamp)-3) || ':00'"))
    print(">rounded to minute")

    idPseudoDic = {}
    generatedStrings = set()
    generatedStrings.add('')
    pseudonymise_udf = udf(lambda z: pseudonymise(z, idPseudoDic, generatedStrings), StringType())
    anonymised = almost_finished.withColumn('anonymId', pseudonymise_udf(array('id', 'week')))

    print(">after pseudonymisation")

    final_sorted = anonymised.sort("numero_ligne", ascending=[True])
    final_sorted.coalesce(1).write.csv(path, header=True, mode="overwrite", sep="\t")


if __name__=='__main__':
    df = readfile("../default.csv")
    #df.show()
    print(">after reading original file")
    anonymize_but_not_completely(6, 22, df, 0.0001, "anonymFrangipane.csv")
