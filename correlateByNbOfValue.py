from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, date_format, length
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import tojson

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

modified_df = spark.read.csv("../autofill_444_clean.csv", schema=schema, header=False, sep='\t')
#modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True)

print(">after reading anonymized file")


original_df = original_df.withColumnRenamed("identifiant", "idOG")
modified_df = modified_df.withColumnRenamed("identifiant", "idAno")
print(">after re-naming columns")

original_df = original_df.withColumn("monthOG_week", date_format("timestamp", "yyyy-MM"))
modified_df = modified_df.withColumn("monthAno_week", date_format("timestamp", "yyyy-MM"))
print(">after selecting month-year")
original_counts = original_df.groupBy("idOg","monthOG_week").agg(count("*").alias("count_original"))
modified_counts = modified_df.groupBy("idAno","monthAno_week" ).agg(count("*").alias("count_anonym"))
print(">after counting lines in file by month and id")

original_counts = original_counts.dropDuplicates(["count_original"])
modified_counts = modified_counts.dropDuplicates(["count_anonym"])
print('>after selecting uniq counts in file')

joined_df = original_counts.join(modified_counts, col("count_original") == col("count_anonym")).where(col("monthOG_week") == col("monthAno_week") )
print(">after joining lines in file with same count")

lines_matching =joined_df.select("idOg", "monthOG_week", "idAno")

CSVManager.writeTabCSVFile(lines_matching.toPandas(),"correlate")
#tojson.dataframeToJson(lines_matching,"autofill444", modified_df.count(),modified_df)

# Arrêter la session Spark
spark.stop()
