from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

# Initialiser SparkSession
spark = SparkSession.builder.appName("Correlation Analysis").getOrCreate()

# Définir le schéma de la donnée
schema = StructType([
    StructField("identifiant", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

# Charger les fichiers CSV avec le schéma spécifié
original_df = spark.read.csv("../original.csv", schema=schema, header=True)
modified_df = spark.read.csv("../ALANOZY_545.csv", schema=schema, header=True)

# Rejoindre les DataFrames sur la colonne 'identifiant'
joined_df = original_df.join(modified_df, "identifiant")

# Écrire le résultat dans un fichier correlation.txt
joined_df.write.mode("overwrite").csv("correspondance.txt", header=True)


# Arrêter la session Spark
spark.stop()

####### MARCHE PAS POUR LE MOMENT 

