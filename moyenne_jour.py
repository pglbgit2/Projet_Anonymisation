import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, coalesce, collect_list, lit, when, date_trunc, avg, hour, rand, rand, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode, from_unixtime, unix_timestamp, round, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas as pd
from itertools import tee

threshold=0.001

def readfile(path: str):
    spark = SparkSession.builder.appName("loader")\
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
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id()), spark

def calcule_avgCoord(df):
    # Filtrer les données pour ne garder que les jours du week-end
    df_weekend = df.filter(dayofweek(df.timestamp).isin([6, 7]))

    # Créer une fenêtre de 3 heures et grouper par cette fenêtre et le jour
    df_avg = df_weekend.groupBy(window(df_weekend.timestamp, "3 hours"), dayofweek(df_weekend.timestamp).alias('day'), "id").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))

    # Extraire l'heure de début de la fenêtre pour faciliter la jointure plus tard
    df_avg = df_avg.withColumn("hour", hour(df_avg["window.start"]))

    return df_avg

    

if __name__ == '__main__':
    dfVic, spark = readfile("victime.csv")
    dfDef, spark = readfile("../default.csv")

    dfVic_avg = calcule_avgCoord(dfVic)
    dfDef_avg = calcule_avgCoord(dfDef)

    # Renommer les colonnes avant de faire la jointure
    dfVic_avg = dfVic_avg.withColumnRenamed("avg_longitude", "vic_avg_longitude").withColumnRenamed("avg_latitude", "vic_avg_latitude")
    dfDef_avg = dfDef_avg.withColumnRenamed("avg_longitude", "def_avg_longitude").withColumnRenamed("avg_latitude", "def_avg_latitude")

    # Joindre les deux DataFrames sur l'heure
    dfJoined = dfVic_avg.join(dfDef_avg, ["hour", "day"], "inner")

    # Calculer la distance entre les moyennes des coordonnées GPS
    dfJoined = dfJoined.withColumn("distance", ((dfJoined.vic_avg_longitude - dfJoined.def_avg_longitude) ** 2 + (dfJoined.vic_avg_latitude - dfJoined.def_avg_latitude) ** 2) ** 0.5)

    # Filtrer les résultats pour ne garder que les ID où la distance est inférieure à un certain seuil
    dfJoined = dfJoined.filter(dfJoined.distance < threshold)

    # Afficher les résultats
    dfJoined.show(100)

#TODO: Rassembler pour chaque semaine les id proches pour chaque intervalles 
#      et les mettre dans un fichier json
#      afficher la moyennes des distances pour les jointures qui ont amené des résultats afin de determiner l'efficacité de l'attaque

