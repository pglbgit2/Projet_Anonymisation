from functools import reduce

from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, weekofyear, abs, col, month, count, first
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

def correlateByAvgLocalisation(Ogfile, Anonymfile, precision):
    # Charger les deux DataFrames à partir des fichiers ou de toute autre source
    df1 = spark.read.csv(Ogfile, header=False, schema=schema, sep='\t')
    print(">after reading original file")

    df2 = spark.read.csv(Anonymfile, header=False, schema=schema, sep='\t' )
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

    seuil_precision = precision  

    filtered_result_df = result_df.filter((abs(result_df.avg_longitude_df1 - result_df.avg_longitude_df2) <= seuil_precision) & (abs(result_df.avg_latitude_df1 - result_df.avg_latitude_df2) <= seuil_precision))
    print(">after filter with precision")
    filtered_result_df = filtered_result_df.select("idOG", "week", "idAno")
    return filtered_result_df
    #CSVManager.writeTabCSVFile(filtered_result_df.toPandas(),"correlationByAvg2")

# column must be: idOg, week, idAno
def merge_dataframes(dataframes_list):
    UniqList = []
    for dataframe in dataframes_list:
        dataframe = dataframe.groupBy("week","idAno").agg(count("*").alias("count"), first("idOG").alias("idOG"))
        Uniqdata = dataframe.where(col("count") == 1)
        UniqList.append(Uniqdata)
    print(">after selecting uniques list of values")
    merged_df = reduce(lambda df1, df2: df1.union(df2), UniqList)
    print(">after merging list into one dataframe")
    unique_merged_df = merged_df.dropDuplicates(subset = ["idAno","week"])
    print(">keeping distinct values")
    return unique_merged_df.select("idOG","week","idAno")

DataframeList = []
TestPrecision = [0.001, 0.05, 0.0005]
for p in TestPrecision:
    DataframeList.append(correlateByAvgLocalisation("default.csv", "../autofill_444_clean.csv", p))
merged = merge_dataframes(DataframeList)
print(">before writing...")
CSVManager.writeTabCSVFile(merged.toPandas(),"MergedcorrelationByAvg")