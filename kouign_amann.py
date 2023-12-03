import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, collect_list, lit, when, date_trunc, avg, hour, rand, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode, from_unixtime, unix_timestamp, round
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
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id()), spark
    

def beurre(df, spark):
    conditionHome=((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6))
    conditionWork=(hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6))
    ConditionWeekend=(hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 6))
    dfHome = df.filter(conditionHome)  # Jours du lundi au jeudi (2=lundi selon ChatGPT)
    dfWork = df.filter(conditionWork)
    dfWeekend = df.filter(ConditionWeekend)
    # df = df.withColumn("type", when((conditionHome|conditionWork|ConditionWeekend), col("type")).otherwise("del"))

    # pour tous les ids calculer des différents points of interest
    dfHome_average = dfHome.groupBy("id").avg("latitude", "longitude")
    dfWork_average = dfWork.groupBy("id").avg("latitude", "longitude")
    dfWeekend_average = dfWeekend.groupBy("id").avg("latitude", "longitude")

    # pour tous les ids calculer des différents points of interest
    clustersHome = perform_dbscan_clustering(dfHome_average, PrecisionGPS).withColumnRenamed('ids', 'id')
    # Identifiez les id de bruit
    noise_ids_home = [row['id'] for row in clustersHome.filter(clustersHome.cluster == -1).select('id').collect()]
    clustersHome = clustersHome.filter(clustersHome.cluster != -1)
    clustersWork = perform_dbscan_clustering(dfWork_average, PrecisionGPS).withColumnRenamed('ids', 'id')
    noise_ids_Work = [row['id'] for row in clustersWork.filter(clustersWork.cluster == -1).select('id').collect()]
    clustersWork = clustersWork.filter(clustersWork.cluster != -1)
    clustersWeekend = perform_dbscan_clustering(dfWeekend_average, PrecisionGPS).withColumnRenamed('ids', 'id')
    noise_ids_Weekend = [row['id'] for row in clustersWeekend.filter(clustersWeekend.cluster == -1).select('id').collect()]
    clustersWeekend = clustersWeekend.filter(clustersWeekend.cluster != -1)
    
    
	# calculer une moyenne de tout les POI SAUF LES MARQUÉS(cf couples seuls)
    
    # Explode la colonne 'ids' pour créer une nouvelle ligne pour chaque id
    clustersHome_exploded = clustersHome.select('cluster', explode('id').alias('id'))
    clustersWork_exploded = clustersWork.select('cluster', explode('id').alias('id'))
    clustersWeekend_exploded = clustersWeekend.select('cluster', explode('id').alias('id'))
    # Joindre df_average avec clusters_exploded
    dfHome_average_clustered = dfHome_average.join(clustersHome_exploded, 'id', 'inner')
    dfWork_average_clustered = dfWork_average.join(clustersWork_exploded, 'id', 'inner')
    dfWeekend_average_clustered = dfWeekend_average.join(clustersWeekend_exploded, 'id', 'inner')
    # Collecter tous les ID pour chaque cluster
    id_listHome = dfHome_average_clustered.groupBy('cluster').agg(collect_list('id').alias('ids'))

    # Calculer la moyenne des latitudes et longitudes pour chaque cluster
    moyenneHome = dfHome_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)')

    # Joindre id_list avec moyenneHome
    moyenneHome = moyenneHome.join(id_listHome, on='cluster', how='right')
    moyenneHome = moyenneHome.select('cluster', 'avg(avg(latitude))', 'avg(avg(longitude))', explode('ids').alias('new_id'))
    # Collecter tous les ID pour chaque cluster
    id_listWork = dfWork_average_clustered.groupBy('cluster').agg(collect_list('id').alias('ids'))
    # Calculer la moyenne des latitudes et longitudes pour chaque cluster
    moyenneWork = dfWork_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)')
    # Joindre id_list avec moyenneWork
    moyenneWork = moyenneWork.join(id_listWork, on='cluster', how='right')
    moyenneWork.show()
    moyenneWork = moyenneWork.select('cluster', 'avg(avg(latitude))', 'avg(avg(longitude))', explode('ids').alias('new_id'))

    # Collecter tous les ID pour chaque cluster
    id_listWeekend = dfWeekend_average_clustered.groupBy('cluster').agg(collect_list('id').alias('ids'))
    # Calculer la moyenne des latitudes et longitudes pour chaque cluster
    moyenneWeekend = dfWeekend_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)')
    # Joindre id_list avec moyenneWeekend
    moyenneWeekend = moyenneWeekend.join(id_listWeekend, on='cluster', how='right')
    moyenneWeekend.show()
    moyenneWeekend = moyenneWeekend.select('cluster', 'avg(avg(latitude))', 'avg(avg(longitude))', explode('ids').alias('new_id'))
    # Renommer les colonnes 'avg(avg(latitude))' et 'avg(avg(longitude))' en 'latitude' et 'longitude'
    moyenneHome = moyenneHome.withColumnRenamed('avg(avg(latitude))', 'latitude').withColumnRenamed('avg(avg(longitude))', 'longitude')
    # Renommer la colonne 'new_id' en 'id'
    moyenneHome = moyenneHome.withColumnRenamed('new_id', 'id')
    moyenneHome = moyenneHome.withColumnRenamed('latitude', 'latMoy').withColumnRenamed('longitude', 'longMoy')
    # Faire une jointure de dfHome avec moyenneHome sur la colonne 'id'
    dfHome = dfHome.join(moyenneHome, on='id', how='left')
    # Créer de nouvelles colonnes 'latitude' et 'longitude' qui contiennent les valeurs de moyenneHome si elles existent, sinon les valeurs de dfHome
    dfHome = dfHome.withColumn('new_latitude', coalesce(moyenneHome['latMoy'], dfHome['latitude']))
    dfHome = dfHome.withColumn('new_longitude', coalesce(moyenneHome['longMoy'], dfHome['longitude']))
    # Supprimer les anciennes colonnes 'latitude' et 'longitude'
    dfHome = dfHome.drop('latitude', 'longitude', 'latMoy', 'longMoy')
    dfHome = dfHome.drop('cluster')

    # Convertir la liste en DataFrame
    noise_ids_home_df = spark.createDataFrame(noise_ids_home, StringType()).toDF("id")
    # Faire une jointure avec dfHome
    dfHome = dfHome.join(noise_ids_home_df, on="id", how="left")
    # Remplacer les valeurs de latitude par 0 pour les id dans noise_ids_home
    dfHome = dfHome.withColumn("suppr", when(dfHome["id"].isin(noise_ids_home_df.select("id").rdd.flatMap(lambda x: x).collect()), 0).otherwise(None))

    dfHome.show()
    dfHome = dfHome.drop('id', 'timestamp')
    # Effectuez le join sur le DataFrame temporaire
    df = df.join(dfHome, on='numero_ligne', how='left')    
    df = df.withColumn('latitude', coalesce(df['new_latitude'], df['latitude']))
    df = df.withColumn('longitude', coalesce(df['new_longitude'], df['longitude']))
    df = df.drop('new_latitude', 'new_longitude')
    df.show()
    # Renommer les colonnes 'avg(avg(latitude))' et 'avg(avg(longitude))' en 'latitude' et 'longitude'
    moyenneWork = moyenneWork.withColumnRenamed('avg(avg(latitude))', 'latitude').withColumnRenamed('avg(avg(longitude))', 'longitude')
    # Renommer la colonne 'new_id' en 'id'
    moyenneWork = moyenneWork.withColumnRenamed('new_id', 'id')
    moyenneWork = moyenneWork.withColumnRenamed('latitude', 'latMoy').withColumnRenamed('longitude', 'longMoy')
    # Faire une jointure de dfWork avec moyenneWork sur la colonne 'id'
    dfWork = dfWork.join(moyenneWork, on='id', how='left')
    # Créer de nouvelles colonnes 'latitude' et 'longitude' qui contiennent les valeurs de moyenneWork si elles existent, sinon les valeurs de dfWork
    dfWork = dfWork.withColumn('new_latitude', coalesce(moyenneWork['latMoy'], dfWork['latitude']))
    dfWork = dfWork.withColumn('new_longitude', coalesce(moyenneWork['longMoy'], dfWork['longitude']))
    # Supprimer les anciennes colonnes 'latitude' et 'longitude'
    dfWork = dfWork.drop('latitude', 'longitude', 'latMoy', 'longMoy')
    dfWork = dfWork.drop('cluster')

    # Convertir la liste en DataFrame
    noise_ids_Work_df = spark.createDataFrame(noise_ids_Work, StringType()).toDF("id")
    # Faire une jointure avec dfWork
    dfWork = dfWork.join(noise_ids_Work_df, on="id", how="left")
    # Remplacer les valeurs de latitude par 0 pour les id dans noise_ids_Work
    dfWork = dfWork.withColumn("suppr1", when(dfWork["id"].isin(noise_ids_Work_df.select("id").rdd.flatMap(lambda x: x).collect()), 0).otherwise(None))

    dfWork.show()
    dfWork = dfWork.drop('id', 'timestamp')
    # Effectuez le join sur le DataFrame temporaire
    df = df.join(dfWork, on='numero_ligne', how='left')    
    df = df.withColumn('latitude', coalesce(df['new_latitude'], df['latitude']))
    df = df.withColumn('longitude', coalesce(df['new_longitude'], df['longitude']))
    df = df.drop('new_latitude', 'new_longitude')


    # Renommer les colonnes 'avg(avg(latitude))' et 'avg(avg(longitude))' en 'latitude' et 'longitude'
    moyenneWeekend = moyenneWeekend.withColumnRenamed('avg(avg(latitude))', 'latitude').withColumnRenamed('avg(avg(longitude))', 'longitude')
    # Renommer la colonne 'new_id' en 'id'
    moyenneWeekend = moyenneWeekend.withColumnRenamed('new_id', 'id')
    moyenneWeekend = moyenneWeekend.withColumnRenamed('latitude', 'latMoy').withColumnRenamed('longitude', 'longMoy')
    # Faire une jointure de dfWeekend avec moyenneWeekend sur la colonne 'id'
    dfWeekend = dfWeekend.join(moyenneWeekend, on='id', how='left')
    # Créer de nouvelles colonnes 'latitude' et 'longitude' qui contiennent les valeurs de moyenneWeekend si elles existent, sinon les valeurs de dfWeekend
    dfWeekend = dfWeekend.withColumn('new_latitude', coalesce(moyenneWeekend['latMoy'], dfWeekend['latitude']))
    dfWeekend = dfWeekend.withColumn('new_longitude', coalesce(moyenneWeekend['longMoy'], dfWeekend['longitude']))
    # Supprimer les anciennes colonnes 'latitude' et 'longitude'
    dfWeekend = dfWeekend.drop('latitude', 'longitude', 'latMoy', 'longMoy')
    dfWeekend = dfWeekend.drop('cluster')

    # Convertir la liste en DataFrame
    noise_ids_Weekend_df = spark.createDataFrame(noise_ids_Weekend, StringType()).toDF("id")
    # Faire une jointure avec dfWeekend
    dfWeekend = dfWeekend.join(noise_ids_Weekend_df, on="id", how="left")
    # Remplacer les valeurs de latitude par 0 pour les id dans noise_ids_Weekend
    dfWeekend = dfWeekend.withColumn("suppr2", when(dfWeekend["id"].isin(noise_ids_Weekend_df.select("id").rdd.flatMap(lambda x: x).collect()), 0).otherwise(None))

    dfWeekend.show()
    dfWeekend = dfWeekend.drop('id', 'timestamp')
    # Effectuez le join sur le DataFrame temporaire
    df = df.join(dfWeekend, on='numero_ligne', how='left')    
    df = df.withColumn('latitude', coalesce(df['new_latitude'], df['latitude']))
    df = df.withColumn('longitude', coalesce(df['new_longitude'], df['longitude']))
    df = df.drop('new_latitude', 'new_longitude')


    # ========================Suppression du Noise========================
    print("avant")
    df.show()
    df = df.withColumn('final_suppr', coalesce(df['suppr'], df['suppr1'], df['suppr2']))
    print("après")
    df.show()
    df.show()
    # Assigner l'id "DEL" à toutes les lignes qui ont 0 en latitude
    df = df.withColumn('id', when((df.final_suppr == 0)  & (rand() <= 0.9), "DEL").otherwise(df.id))
    df = df.withColumn('longitude', when(df.final_suppr == 0, round(df.longitude, 2)).otherwise(df.longitude))
    df = df.withColumn('latitude', when(df.final_suppr == 0, round(df.latitude, 2)).otherwise(df.longitude))
    df.show()
    df = df.drop('suppr', 'suppr1', 'suppr2', 'final_suppr')

    # Supprimer les lignes qui ne sont pas calculer dans les POI

    # ========================dfDL========================
    # Création d'un DataFrame avec les lignes à supprimer
    # dfDEL = df.filter(~((hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 6)))
    #                   &~((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))
    #                   &~(((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))
    #                   )
    # # Créer une liste des lignes dans dfDEL
    # dfDEL.show()
    # del_ids = [row['numero_ligne'] for row in dfDEL.select('numero_ligne').distinct().collect()]
    # print("Voici les numéro des lignes qui seront supprimer :",del_ids)
    # # Assigner "DEL" à l'identifiant des lignes de df quai sont dans dfDEL
    # df = df.withColumn('id', when(col('numero_ligne').isin(del_ids), "DEL").otherwise(col('id')))

    df = df.withColumn("id", when((conditionHome|conditionWork|ConditionWeekend), col("id")).otherwise("DEL"))


    # Arrondir le champ 'timestamp' à l'heure la plus proche
    df = df.withColumn('timestamp', date_trunc('hour', 'timestamp'))
    
    # Convertir le champ 'timestamp' en une chaîne de caractères avec le format 'AAAA-MM-JJ HH:MM:SS'
    df = df.withColumn('timestamp', from_unixtime(unix_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss')))

    df = df.sort("numero_ligne", ascending=[True])
    df = df.drop("numero_ligne")



    df.coalesce(1).write.csv("kouign_amann.csv", header=False, mode="overwrite", sep="\t")



if __name__ == '__main__':
    df, spark = readfile("ReferencePseudo.csv/part-00000-0ac17880-ad86-4d78-a892-e1ea4e0447cd-c000.csv")
    beurre(df, spark)



