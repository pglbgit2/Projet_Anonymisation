from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import count, col, date_format, length, weekofyear, abs
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

original_df = spark.read.csv("ReferenceINSA.csv", schema=schema, header=False, sep='\t')
#original_df = spark.read.csv("tableau.csv", schema=schema, header=True)
nb_lignes_ori = original_df.count()
print(">after reading original file")

modified_df = spark.read.csv("files/DataLockers_670_clean.csv", schema=schema, header=False, sep='\t')
#modified_df = spark.read.csv("anonymisedTab.csv", schema=schema, header=True)
nb_lignes_mod = modified_df.count()
print(nb_lignes_ori)
print(nb_lignes_mod)

print(">after reading anonymized file")


original_df = original_df.withColumnRenamed("identifiant", "idOG")
modified_df = modified_df.withColumnRenamed("identifiant", "idAno")
print(">after re-naming columns")

original_df = original_df.withColumn("monthOG_week", weekofyear("timestamp"))
modified_df = modified_df.withColumn("monthAno_week", weekofyear("timestamp"))
print(">after selecting month-year")
original_counts = original_df.groupBy("idOg","monthOG_week").agg(count("*").alias("count_original"))
modified_counts = modified_df.groupBy("idAno","monthAno_week" ).agg(count("*").alias("count_anonym"))
# modified_counts.show()
modified_counts = modified_counts.withColumn("count_anonym", (modified_counts.count_anonym * (nb_lignes_ori/nb_lignes_mod)))
# modified_counts.show()

print(">after counting lines in file by month and id")


# # original_counts = original_counts.dropDuplicates(["count_original"])
# # modified_counts = modified_counts.dropDuplicates(["count_anonym"])
# # print('>after selecting uniq counts in file')

# joined_df = original_counts.join(modified_counts, col("count_original") == col("count_anonym")).where(col("monthOG_week") == col("monthAno_week") )
# print(">after joining lines in file with same count")

# lines_matching =joined_df.select("idOg", "monthOG_week", "idAno")

# CSVManager.writeTabCSVFile(lines_matching.toPandas(),"correlate")
# #tojson.dataframeToJson(lines_matching,"autofill444", modified_df.count(),modified_df)
joined_df = original_counts.join(modified_counts, (original_counts.monthOG_week == modified_counts.monthAno_week), "outer")
joined_df.show()
count_anonym = joined_df.orderBy("idOg", "monthOG_week", abs(joined_df.count_original - joined_df.count_anonym)).drop_duplicates(["idOg", "monthOG_week"])
count_anonym = count_anonym.select("idOg", "monthOG_week", "idAno")

count_anonym.show()

print(">before writing...")
count_anonym.withColumnRenamed("idOG", "ID")    # 3 colonnes : ID, Date, ID_Anon
count_anonym.withColumnRenamed("monthOG_week", "Date")
count_anonym.withColumnRenamed("idAno", "ID_Anon")
mergedpd = count_anonym.toPandas()
idlisttab = original_df.select("idOG").distinct()
idlisttab = idlisttab.toPandas().values.tolist()
idlist = []
for id in idlisttab:
    idlist.append(id[0])
json_out = tojson.dataframeToJSON(mergedpd,True, idlist)
with open("identified670_1.json", "w") as outfile:
    outfile.write(json_out)


# Arrêter la session Spark
spark.stop()
