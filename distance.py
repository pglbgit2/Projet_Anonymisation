from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialisez la session Spark
spark = SparkSession.builder.appName("DistanceCalculations").getOrCreate()

# Chargez les données depuis le fichier CSV en spécifiant le schéma
schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])

df = spark.read.csv("default.csv", sep="\t", header=False, schema=schema)

# Définissez la position géographique de référence pour chaque groupe
reference_latitude = 48.89 # Remplacez par votre latitude de référence
reference_longitude = 2.34  # Remplacez par votre longitude de référence

# Ajoutez une colonne pour la distance à la position de référence
df = df.withColumn("distance_to_reference",
    expr("SQRT(POW(latitude - {0}, 2) + POW(longitude - {1}, 2))".format(reference_latitude, reference_longitude))
)

# Sélectionnez la ligne avec la distance maximale pour chaque identifiant
max_distance_df = df.groupBy("identifiant").max("distance_to_reference")
max_distance_df = max_distance_df.withColumnRenamed("max(distance_to_reference)", "max_distance")

# Sélectionnez également les coordonnées de la position géographique la plus éloignée
coord_df = df.join(max_distance_df, on=["identifiant"]).where(col("distance_to_reference") == col("max_distance"))
coord_df = coord_df.select("identifiant", "max_distance", "longitude", "latitude")

# Enregistrez les résultats dans un fichier CSV
coord_df.write.csv("resultats.csv", header=True)

# Arrêtez la session Spark
spark.stop()

