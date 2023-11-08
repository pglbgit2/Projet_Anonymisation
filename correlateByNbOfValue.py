from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, date_format

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager

spark = SparkSession.builder.appName("Correlation Analysis").getOrCreate()

schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")

original_df = spark.read.csv("default.csv", schema=schema, header=False, sep='\t')
#original_df = spark.read.csv("tableau.csv", schema=schema, header=True)

print(">after reading original file")

#modified_df = spark.read.csv("ALANOZY_545.csv", schema=schema, header=False, sep='\t')
#modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True)

print(">after reading anonymized file")


original_df = original_df.withColumnRenamed("identifiant", "idOG")
modified_df = modified_df.withColumnRenamed("identifiant", "idAno")
print(">after re-naming columns")

original_df = original_df.withColumn("monthOG_year", date_format("timestamp", "yyyy-MM"))
modified_df = modified_df.withColumn("monthAno_year", date_format("timestamp", "yyyy-MM"))
print(">after selecting month-year")

original_counts = original_df.groupBy("idOg","monthOG_year").agg(count("*").alias("count_original"))
modified_counts = modified_df.groupBy("idAno","monthAno_year" ).agg(count("*").alias("count_anonym"))
print(">after counting lines in file by month and id")

original_counts = original_counts.dropDuplicates(["count_original"])
modified_counts = modified_counts.dropDuplicates(["count_anonym"])
print('>after selecting uniq counts in file')

joined_df = original_counts.join(modified_counts, col("count_original") == col("count_anonym"))
print(">after joining lines in file with same count")

lines_matching =joined_df.select("idOg", "idAno", "monthOG_year", "count_original")

CSVManager.writeTabCSVFile(lines_matching.toPandas(),"correlate")


# Arrêter la session Spark
spark.stop()
