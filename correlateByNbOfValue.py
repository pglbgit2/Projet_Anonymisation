from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# Initialiser SparkSession
spark = SparkSession.builder.appName("Comparaison d'identifiants").getOrCreate()

# Charger les fichiers CSV avec le schéma spécifié
schema = StructType([
    StructField("identifiant", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

original_df = spark.read.csv("../original.csv", schema=schema, header=True)
modified_df = spark.read.csv("../ALANOZY_545.csv", schema=schema, header=True)

# Compter le nombre de lignes pour chaque identifiant dans chaque fichier
original_counts = original_df.groupBy("identifiant").agg(count("*").alias("count_original"))
modified_counts = modified_df.groupBy("identifiant").agg(count("*").alias("count_modified"))

# Rejoindre les DataFrames sur l'identifiant et comparer le nombre de lignes
matched_ids = original_counts.join(modified_counts, "identifiant") \
    .filter(col("count_original") == col("count_modified"))

# Écrire les correspondances dans un fichier correspondances.txt
matched_ids.write.mode("overwrite").csv("correlation.txt", header=True)

# Arrêter la session Spark
spark.stop()

