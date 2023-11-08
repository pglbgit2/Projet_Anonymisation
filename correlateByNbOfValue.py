from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, date_format

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
original_df = spark.read.csv("tableau.csv", schema=schema, header=True)
print(">after reading original file")


modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True)
print(">after reading anonymized file")


original_df = original_df.withColumnRenamed("identifiant", "idOG")
modified_df = modified_df.withColumnRenamed("identifiant", "idAno")

print(">after re-naming columns")
original_df = original_df.withColumn("monthOG_year", date_format("timestamp", "yyyy-MM"))
modified_df = modified_df.withColumn("monthAno_year", date_format("timestamp", "yyyy-MM"))

print(">after selecting month-year")

original_counts = original_df.groupBy("idOg","monthOG_year").agg(count("*").alias("count_original"))
modified_counts = modified_df.groupBy("idAno","monthAno_year" ).agg(count("*").alias("count_anonym"))
print(">after counting lines in file")

original_counts = original_counts.dropDuplicates(["count_original"])
modified_counts = modified_counts.dropDuplicates(["count_anonym"])
print('>after selecting uniq counts in file')

joined_df = original_counts.join(modified_counts).where((col("count_original") == col("count_anonym")) & (col("monthOG_year") == col("monthAno_year")))

print(">after joining lines in file with same count")

lines_matching =joined_df.select("idOg", "idAno", "monthOG_year", "count_original")


# Écrire les correspondances dans un fichier correlate
CSVManager.writeTabCSVFile(lines_matching.toPandas(),"correlate")


# Arrêter la session Spark
spark.stop()
