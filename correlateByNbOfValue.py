from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, count, col

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


original_df = original_df.withColumnRenamed("identifiant", "idOG")
modified_df = modified_df.withColumnRenamed("identifiant", "idAno")

print(">after re-naming columns")


# Compter le nombre de lignes pour chaque identifiant dans chaque fichier
original_counts = original_df.groupBy("idOg").agg(count("*").alias("count_original"))
modified_counts = modified_df.groupBy("idAno").agg(count("*").alias("count_modified"))
print(">after counting lines in file")

joined_df = original_counts.join(modified_counts, (col("count_original") == col("count_modified")), "inner")

print(">after matching lines in file")

# Écrire les correspondances dans un fichier correlate
CSVManager.writeTabCSVFile(joined_df.toPandas(),"correlate")


# Arrêter la session Spark
spark.stop()
