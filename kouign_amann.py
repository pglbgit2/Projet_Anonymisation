import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, date_trunc, avg, hour, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode, from_unixtime, unix_timestamp
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
    dfHome = df.filter(((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))  # Jours du lundi au jeudi (2=lundi selon ChatGPT)
    dfWork = df.filter((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))
    dfWeekend = df.filter((hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 6)))


    

    # pour tous les ids calculer des différents points of interest
    dfHome_average = dfHome.groupBy("id").avg("latitude", "longitude")
    dfWork_average = dfWork.groupBy("id").avg("latitude", "longitude")
    dfWeekend_average = dfWeekend.groupBy("id").avg("latitude", "longitude")

    # pour tous les ids calculer des différents points of interest
    clustersHome = perform_dbscan_clustering(dfHome_average, PrecisionGPS).withColumnRenamed('ids', 'id')
    clustersWork = perform_dbscan_clustering(dfWork_average, PrecisionGPS).withColumnRenamed('ids', 'id')
    clustersWeekend = perform_dbscan_clustering(dfWeekend_average, PrecisionGPS).withColumnRenamed('ids', 'id')

    
    
	# calculer une moyenne de tout les POI SAUF LES MARQUÉS(cf couples seuls)
    
    # Explode la colonne 'ids' pour créer une nouvelle ligne pour chaque id
    clustersHome_exploded = clustersHome.select('cluster', explode('id').alias('id'))
    clustersWork_exploded = clustersWork.select('cluster', explode('id').alias('id'))
    clustersWeekend_exploded = clustersWeekend.select('cluster', explode('id').alias('id'))

    # Joindre df_average avec clusters_exploded
    dfHome_average_clustered = dfHome_average.join(clustersHome_exploded, 'id', 'inner')
    dfWork_average_clustered = dfWork_average.join(clustersWork_exploded, 'id', 'inner')
    dfWeekend_average_clustered = dfWeekend_average.join(clustersWeekend_exploded, 'id', 'inner')


    moyenneHome = dfHome_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)').collect()
    moyenneWork = dfWork_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)').collect()
    moyenneWeekend = dfWeekend_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)').collect()

    # Assigner à l'ensemble des lignes concerné, dans les valeurs de localisation, la moyenne calculée
    # Joindre df avec clusters_exploded et supprimer les colonnes 'numero_ligne' et 'cluster'
    
    # Créez une condition pour les lignes qui respectent le filtre Home
    home_condition = ((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 5))
    # Créez un DataFrame temporaire qui contient uniquement les lignes qui respectent le filtre Home
    df_home = df.filter(home_condition)
    # Effectuez le join sur le DataFrame temporaire
    df = df_home.join(clustersHome_exploded, 'id', 'left').drop('cluster')

    # Créez une condition pour les lignes qui respectent le filtre Work
    work_condition = ((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 5))
    # Créez un DataFrame temporaire qui contient uniquement les lignes qui respectent le filtre Work
    df_work = df.filter(work_condition)
    # Effectuez le join sur le DataFrame temporaire
    df = df_work.join(clustersWork_exploded, 'id', 'left').drop('cluster')
    
    # Créez une condition pour les lignes qui respectent le filtre Weekend
    weekend_condition = ((hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18)) & ((dayofweek(df["timestamp"]) == 6) | (dayofweek(df["timestamp"]) == 7))
    # Créez un DataFrame temporaire qui contient uniquement les lignes qui respectent le filtre Weekend
    df_weekend = df.filter(weekend_condition)
    # Effectuez le join sur le DataFrame temporaire
    df = df_weekend.join(clustersWeekend_exploded, 'id', 'left').drop('cluster')

    # Identifiez les id de bruit
    noise_ids_home = [row['id'] for row in clustersHome.filter(clustersHome.cluster == -1).select('id').collect()]
    noise_ids_work = [row['id'] for row in clustersWork.filter(clustersWork.cluster == -1).select('id').collect()]
    noise_ids_weekend = [row['id'] for row in clustersWeekend.filter(clustersWeekend.cluster == -1).select('id').collect()]

    print("Noise_Home", noise_ids_home)
    print("Noise_Work", noise_ids_work)
    print("Noise_Weekend", noise_ids_weekend)

    # Convertir les listes en DataFrames
    noise_ids_home_df = spark.createDataFrame(noise_ids_home, StringType()).toDF('home_id')
    noise_ids_work_df = spark.createDataFrame(noise_ids_work, StringType()).toDF('work_id')
    noise_ids_weekend_df = spark.createDataFrame(noise_ids_weekend, StringType()).toDF('weekend_id')
    noise_ids_home_df.show()
    # Assigner les coordonnées à 0 pour les id de bruit
    df = df.join(noise_ids_home_df, df.id == noise_ids_home_df.home_id, 'left_outer') \
        .withColumn('latitude', when((col('home_id').isNotNull()) & 
                         ((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & 
                         ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)), 
                         0).otherwise(df.latitude)) \
        .drop(noise_ids_home_df.home_id)
    # ---------------------DEBUG--------------------
    # Créez une condition pour les lignes qui seront modifiées
    condition = ((col('home_id').isNotNull()) & 
                ((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & 
                ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))

    # Créez un DataFrame temporaire qui contient uniquement les lignes qui seront modifiées
    temp_df = df.join(noise_ids_home_df, df.id == noise_ids_home_df.home_id, 'left_outer').filter(condition)

    # Affichez les lignes du DataFrame temporaire
    temp_df.show()
    # ---------------------DEBUG--------------------
    df = df.join(noise_ids_work_df, df.id == noise_ids_work_df.work_id, 'left_outer') \
        .withColumn('latitude', when((col('work_id').isNotNull()) &
                         ((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & 
                         ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6))), 
                         0).otherwise(df.latitude)) \
        .drop(noise_ids_work_df.work_id)

    df = df.join(noise_ids_weekend_df, df.id == noise_ids_weekend_df.weekend_id, 'left_outer') \
        .withColumn('latitude', when((col('weekend_id').isNotNull()) &
                         (hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & 
                         ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 6)),
                         0).otherwise(df.latitude)) \
        .drop(noise_ids_weekend_df.weekend_id)
    # Assigner l'id "DEL" à toutes les lignes qui ont 0 en latitude
    df = df.withColumn('id', when(df.latitude == 0, "DEL").otherwise(df.id))

    # Supprimer les lignes qui ne sont pas calculer dans les POI

    # ===================dfDL A REPLACER ICI===================
    # Création d'un DataFrame avec les lignes à supprimer
    dfDEL = df.filter(~((hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 6)))
                      &~((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))
                      &~(((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6)))
                      )    
    # Créer une liste des lignes dans dfDEL
    dfDEL.show()
    del_ids = [row['numero_ligne'] for row in dfDEL.select('numero_ligne').distinct().collect()]
    print("Voici les numéro des lignes qui seront supprimer :",del_ids)
    # Assigner "DEL" à l'identifiant des lignes de df quai sont dans dfDEL
    df = df.withColumn('id', when(col('numero_ligne').isin(del_ids), "DEL").otherwise(col('id')))

    # Arrondir le champ 'timestamp' à l'heure la plus proche
    df = df.withColumn('timestamp', date_trunc('hour', 'timestamp'))
    
    # Convertir le champ 'timestamp' en une chaîne de caractères avec le format 'AAAA-MM-JJ HH:MM:SS'
    df = df.withColumn('timestamp', from_unixtime(unix_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss')))

    df = df.sort("numero_ligne", ascending=[True])
    df = df.drop("numero_ligne")



    df.coalesce(1).write.csv("kouign_amann.csv", header=False, mode="overwrite", sep="\t")



if __name__ == '__main__':
    df, spark = readfile("res.csv")
    beurre(df, spark)



