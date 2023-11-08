from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, weekofyear, abs, month
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
spark = SparkSession.builder.appName("CalculatingAveragePosition").getOrCreate()


schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")

# Charger les deux DataFrames à partir des fichiers ou de toute autre source
df1 = spark.read.csv("default.csv", header=False, schema=schema)
print(">after reading original file")

df2 = spark.read.csv("../autofill_444_clean.csv", header=False, schema=schema)
print(">after reading anonymized file")


# Extraire le mois du timestamp
df1 = df1.withColumn("week", weekofyear("timestamp"))
df2 = df2.withColumn("week", weekofyear("timestamp"))
print(">after week extraction")


df1 = df1.withColumnRenamed("identifiant", "idOG")
df2 = df2.withColumnRenamed("identifiant", "idAno")
print(">after re-naming columns")
# Calculer la position moyenne par semaine dans chaque DataFrame
avg_position_df1 = df1.groupBy("idOG", "week").agg(avg("longitude").alias("avg_longitude_df1"), avg("latitude").alias("avg_latitude_df1"))
avg_position_df2 = df2.groupBy("idAno", "week").agg(avg("longitude").alias("avg_longitude_df2"), avg("latitude").alias("avg_latitude_df2"))
print(">after average localisation extraction")


# Joindre les deux DataFrames sur la colonne "month"
result_df = avg_position_df1.join(avg_position_df2, "week", "inner").select("idOG", "week", "idAno","avg_latitude_df1","avg_latitude_df2","avg_longitude_df1","avg_longitude_df2")

print(">after join")

seuil_precision = 0.00001  

filtered_result_df = result_df.filter((abs(result_df.avg_longitude_df1 - result_df.avg_longitude_df2) <= seuil_precision) & (abs(result_df.avg_latitude_df1 - result_df.avg_latitude_df2) <= seuil_precision))
print(">after filter with precision")
filtered_result_df = filtered_result_df.select("idOG", "week", "idAno")
# Afficher le résultat
CSVManager.writeTabCSVFile(filtered_result_df.toPandas(),"correlationByAvg")

