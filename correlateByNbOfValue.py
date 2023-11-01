from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

# Initialiser SparkSession
spark = SparkSession.builder.appName("Comparaison d'identifiants").getOrCreate()

# Définir le schéma de la donnée
schema = StructType([
    StructField("identifiant", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")


original_df = spark.read.csv("tableau.csv", schema=schema, header=True)
modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True)
print(">after reading file")


# Arrêter la session Spark
spark.stop()

