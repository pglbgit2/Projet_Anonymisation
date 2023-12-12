import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, coalesce, collect_list, lit, when, date_trunc, avg, hour, rand, rand, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode, from_unixtime, unix_timestamp, round, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window
import CSVManager
import pandas as pd
from itertools import tee
import tojson

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
    #df_weekend = df.filter(dayofweek(df.timestamp).isin([6, 7]))
    df_weekend = df
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
    dfVic_avg = dfVic_avg.withColumnRenamed("window", "vic_window").withColumnRenamed("avg_longitude", "vic_avg_longitude").withColumnRenamed("avg_latitude", "vic_avg_latitude").withColumnRenamed("id", "vic_id")
    dfDef_avg = dfDef_avg.withColumnRenamed("window", "def_window").withColumnRenamed("avg_longitude", "def_avg_longitude").withColumnRenamed("avg_latitude", "def_avg_latitude").withColumnRenamed("id", "def_id")

    # Joindre les deux DataFrames sur l'heure
    dfJoined = dfDef_avg.join(dfVic_avg, ["hour", "day"], "inner")

    # Calculer la distance entre les moyennes des coordonnées GPS
    dfJoined = dfJoined.withColumn("distance", ((dfJoined.vic_avg_longitude - dfJoined.def_avg_longitude) ** 2 + (dfJoined.vic_avg_latitude - dfJoined.def_avg_latitude) ** 2) ** 0.5)

    # Ajouter la semaine à dfJoined
    dfJoined = dfJoined.withColumn("week", weekofyear(dfJoined["vic_window.start"]))

    # Calculer le rang de chaque ligne en fonction de la distance, pour chaque semaine et chaque "def_id"
    windowSpec = Window.partitionBy("week", "def_id").orderBy("distance")
    dfJoined = dfJoined.withColumn("rank", row_number().over(windowSpec))

    # Sélectionner uniquement les lignes avec le rang 1, c'est-à-dire la distance minimale pour chaque semaine et chaque "def_id"
    dfMinDistance = dfJoined.filter(dfJoined.rank == 1)

    # Sélectionner uniquement les colonnes nécessaires
    dfFinal = dfMinDistance.select("vic_id", "def_id", "week", "distance")

    # Afficher les résultats
    dfFinal.show(100)
    dfFinal = dfFinal.drop("distance")
    dfFinal = dfFinal.select("def_id", "week", "vic_id")
    dfFinal.withColumnRenamed("def_id", "ID")    # 3 colonnes : ID, Date, ID_Anon
    dfFinal.withColumnRenamed("week", "Date")
    dfFinal.withColumnRenamed("vic_id", "ID_Anon")
    mergedpd = dfFinal.toPandas()
    idlisttab = dfDef.select("id").distinct()
    idlisttab = idlisttab.toPandas().values.tolist()
    idlist = []
    for id in idlisttab:
        idlist.append(id[0])
    json_out = tojson.dataframeToJSON(mergedpd,True, idlist)
    with open("resultat.json", "w") as outfile:
        outfile.write(json_out)

#TODO: Rassembler pour chaque semaine les id proches pour chaque intervalles 
#      et les mettre dans un fichier json
#      afficher la moyennes des distances pour les jointures qui ont amené des résultats afin de determiner l'efficacité de l'attaque



# Inverser la jointure de les id default sur victime plutôt que victime sur default
# Vérifier qu'un id n'est pas présent sur plusieurs semaines côté victime

