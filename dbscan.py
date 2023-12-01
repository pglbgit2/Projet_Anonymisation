# pip3 install scikit-learn
from sklearn.cluster import DBSCAN
from pyspark.sql.functions import udf, collect_list, monotonically_increasing_id
from pyspark.sql.types import IntegerType

def perform_dbscan_clustering(df, PrecisionGPS):
    # Ajouter une colonne d'index à df
    df = df.withColumn('index', monotonically_increasing_id())

    # Préparer les données
    data = df.select('avg(latitude)', 'avg(longitude)').rdd.map(lambda row: (row[0], row[1])).collect()

    # Exécuter DBSCAN
    db = DBSCAN(eps=PrecisionGPS, min_samples=2).fit(data)

    # Convertir les labels de cluster en une liste Python
    labels = db.labels_.tolist()

    # Convertir les labels de cluster en une colonne Spark
    labels_udf = udf(lambda x: labels[x], IntegerType())
    df = df.withColumn('cluster', labels_udf(df['index']))

    # Regrouper par cluster et collecter les identifiants
    clusters = df.groupby('cluster').agg(collect_list('id').alias('ids'))

    return clusters