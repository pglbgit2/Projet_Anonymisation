from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
import CSVManager
# Initialiser SparkSession
spark = SparkSession.builder.appName("Correlation Analysis").getOrCreate()

# Définir le schéma de la donnée
schema = StructType([
    StructField("identifiant", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")
# Charger les fichiers CSV avec le schéma spécifié
original_df = spark.read.csv("tableau.csv", schema=schema, header=True)
modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True)
print(">after reading file")

id_OG = original_df.select("identifiant").withColumnRenamed("identifiant", "idOG")
id_Anonym = modified_df.select("identifiant").withColumnRenamed("identifiant", "idAno")

# Effectuez le joint sur la colonne "identifiant"
joined_df = id_OG.join(id_Anonym, col("idOG") == col("idAno"), "inner")

print(">after join")

# Écrire le résultat dans un fichier correlation.txt
CSVManager.writeTabCSVFile(joined_df.toPandas(),"correspondance")
print(">after write file")


# Arrêter la session Spark
spark.stop()

####### MARCHE PAS POUR LE MOMENT 

