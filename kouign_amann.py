import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode
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

    # pour tous les ids calculer des différents points of interest
    clustersHome = perform_dbscan_clustering(dfHome_average, PrecisionGPS)
    clustersWork = perform_dbscan_clustering(dfWork_average, PrecisionGPS)
    clustersWeekend = perform_dbscan_clustering(dfWeekend_average, PrecisionGPS)

	# calculer une moyenne de tout les POI SAUF LES MARQUÉS(cf couples seuls)
    
    # Explode la colonne 'ids' pour créer une nouvelle ligne pour chaque id
    clustersHome_exploded = clustersHome.select('cluster', explode('ids').alias('id'))
    clustersWork_exploded = clustersWork.select('cluster', explode('ids').alias('id'))
    clustersWeekend_exploded = clustersWeekend.select('cluster', explode('ids').alias('id'))

    # Joindre df_average avec clusters_exploded
    dfHome_average_clustered = dfHome_average.join(clustersHome_exploded, 'id', 'inner')
    dfWork_average_clustered = dfWork_average.join(clustersWork_exploded, 'id', 'inner')
    dfWeekend_average_clustered = dfWeekend_average.join(clustersWeekend_exploded, 'id', 'inner')

    # Calculer la moyenne des latitudes et longitudes pour chaque cluster
    moyenneHome = dfHome_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)').collect()
    moyenneWork = dfWork_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)').collect()
    moyenneWeekend = dfWeekend_average_clustered.groupBy('cluster').avg('avg(latitude)', 'avg(longitude)').collect()
	
    # Assigner à l'ensemble des lignes concerné, dans les valeurs de localisation, la moyenne calculé
    # Joindre df avec clusters_exploded et supprimer les colonnes 'numero_ligne' et 'cluster'
    df = df.join(clustersHome_exploded, 'id', 'left').drop('numero_ligne', 'cluster')
    df = df.join(clustersWork_exploded, 'id', 'left').drop('numero_ligne', 'cluster')
    df = df.join(clustersWeekend_exploded, 'id', 'left').drop('numero_ligne', 'cluster')

    # Écrire le DataFrame dans un fichier CSV
    df.coalesce(1).write.csv("kouign_amann.csv", header=True,  mode="overwrite", sep="\t")



if __name__ == '__main__':
    df = readfile("res.csv")
    beurre(df)



