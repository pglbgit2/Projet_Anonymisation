import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas as pd
from itertools import tee
from dbscan import perform_dbscan_clustering

PrecisionGPS = 0.008
# 1km = 0.008983 degrés de latitude/longitude


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
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id())
    

def beurre(df):
    dfHome = df.filter(((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 5)))  # Jours du lundi au jeudi (2=lundi selon ChatGPT)
    dfWork = df.filter((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 5)))
    dfWeekend = df.filter((hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 5)))

    # pour tous les ids calculer des différents points of interest
    dfHome_average = dfHome.groupBy("id").avg("latitude", "longitude")
    dfWork_average = dfWork.groupBy("id").avg("latitude", "longitude")
    dfWeekend_average = dfWeekend.groupBy("id").avg("latitude", "longitude")

    clusters = perform_dbscan_clustering(dfHome_average, PrecisionGPS)
    print(clusters.collect())
    # print("Nombre de personnes Home : " + str(dfHome_average.count()))
    # print("Nombre de personnes Work : " + str(dfWork_average.count()))
    # print("Nombre de personnes Weekend : " + str(dfWeekend_average.count()))
    # # rassembler les couples (id;POI) qui sont proches (cf: distance utility)
    # result_list_home = dfHome_average.rdd.mapPartitions(rassembleur).collect()
    # result_list_work = dfWork_average.rdd.mapPartitions(rassembleur).collect()
    # result_list_weekend = dfWeekend_average.rdd.mapPartitions(rassembleur).collect()

    # # Afficher les résultats
    # print("Voici la liste Home :" + str(result_list_home))
    # print("Voici la liste Work :" + str(result_list_work))
    # print("Voici la liste Weekend :" + str(result_list_weekend))
    # print("Nombre de personnes Home : " + str(sum(len(ensemble) for ensemble in result_list_home)))
    # print("Nombre de personnes Work : " + str(sum(len(ensemble) for ensemble in result_list_work)))
    # print("Nombre de personnes Weekend : " + str(sum(len(ensemble) for ensemble in result_list_weekend)))
    # ---------------------------------------------------------------------------------------------------------------
    # Les résultats ne semblent pas cohérents, il y a 10 fois moins de personnes rassemblé dans un même lieu de travail
    # que de personnes qui existantes dans le fichier de base. Il y a un seul cluster quel que soit la précision
    # Problème de précision des coordonnées GPS ?
    # ---------------------------------------------------------------------------------------------------------------

        
# def rassembleur(iterator):
#     liste = []
#     cmp = 0
#     # Dupliquer l'itérateur
#     iterator1, iterator2 = tee(iterator)
#     for row1 in iterator1:
#         # si id est déjà traité
#         if any(row1["id"] in ensemble for ensemble in liste):
#             continue

#         # Avancer l'itérateur2 pour qu'il soit aligné avec l'itérateur1
#         next(iterator2, None)

#         for row2 in iterator2:
#             if row1["id"] != row2["id"] and distanceProche(row1, row2):
#                 cmp += 1
#                 # Recherche de l'ensemble cible en fonction de la chaîne
#                 ensemble_cible = None
#                 for ensemble in liste:
#                     if row1["id"] in ensemble or row2["id"] in ensemble:
#                         ensemble_cible = ensemble
#                         break  # Arrêter la recherche une fois que l'ensemble cible est trouvé
#                 # Si l'ensemble cible est trouvé, ajouter les deux id à l'intérieur
#                 if ensemble_cible is not None:
#                     if row1["id"] not in ensemble_cible:
#                         ensemble_cible.add(row1["id"])
#                     if row2["id"] not in ensemble_cible:
#                         ensemble_cible.add(row2["id"])
#                 # Si aucun ensemble cible n'a été trouvé, créer un nouvel ensemble
#                 elif ensemble_cible is None:
#                     nouvel_ensemble = set([row1["id"], row2["id"]])
#                     liste.append(nouvel_ensemble)
#     print("Compteur : " + str(cmp))
#     return liste
#     # TODO: Refaire la boucle pour les id qui n'ont pas été traités



def distanceProche(row1, row2):
    if row1["avg(latitude)"] - row2["avg(latitude)"] <= PrecisionGPS and row1["avg(longitude)"] - row2["avg(longitude)"] <= PrecisionGPS:
        return True
    else:
        return False

if __name__ == '__main__':
    df = readfile("res.csv")
    beurre(df)


