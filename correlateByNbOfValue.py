from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
# Initialiser SparkSession
spark = SparkSession.builder.appName("Correlation Analysis").getOrCreate()

# Définir le schéma de la donnée
schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")
# Charger les fichiers CSV avec le schéma spécifié
original_df = spark.read.csv("tableau.csv", schema=schema, header=True).withColumn("Ogrow_number", monotonically_increasing_id()+2)
print(">after reading original file")


modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True).withColumn("Anonymrow_number", monotonically_increasing_id()+2)
print(">after reading anonymized file")

# Arrêter la session Spark
spark.stop()
