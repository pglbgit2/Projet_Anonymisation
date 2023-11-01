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
original_df = spark.read.csv("default.csv", schema=schema, header=True).withColumn("Ogrow_number", monotonically_increasing_id()+2)
print(">after reading original file")


modified_df = spark.read.csv("ALANOZY_545.csv", schema=schema, header=True).withColumn("Anonymrow_number", monotonically_increasing_id()+2)
print(">after reading anonymized file")


id_OG = original_df.select("identifiant","Ogrow_number").withColumnRenamed("identifiant", "idOG")
id_Anonym = modified_df.select("identifiant","Anonymrow_number").withColumnRenamed("identifiant", "idAno")

# Effectuez le joint sur les colonnes "idOG" et "idAno"
joined_df = id_OG.join(id_Anonym, (id_OG.idOG == id_Anonym.idAno), "inner")

print(">after join")

# Écrire le résultat dans un fichier correlation.txt
CSVManager.writeTabCSVFile(joined_df.toPandas(),"correspondance")
print(">after write file")


# Arrêter la session Spark
spark.stop()
