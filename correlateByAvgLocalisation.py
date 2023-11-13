from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, weekofyear, abs, col, month, count, first, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import tojson
spark = SparkSession.builder.appName("CalculatingAveragePosition").getOrCreate()


schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True)
])
print(">after schema definition")
# Charger les deux DataFrames à partir des fichiers ou de toute autre source
df1 = spark.read.csv("../default.csv", header=False, schema=schema, sep='\t')
print(">after reading original file")

df2 = spark.read.csv("../ALANOZY_545.csv", header=False, schema=schema, sep='\t' )
print(">after reading anonymized file")
df1 = df1.withColumn("week", weekofyear("timestamp"))
df2 = df2.withColumn("week", weekofyear("timestamp"))
print(">after week extraction")


df1 = df1.withColumnRenamed("identifiant", "idOG")
df2 = df2.withColumnRenamed("identifiant", "idAno")
print(">after re-naming columns")

# selectionne d'abord uniquement les points s'éloignant plus qu'une certaine distance limite avant de corréler par moyenne
# l'objectif est de se débarasser des points souvent fréquenté par tout le monde et de sélectionner uniquement ceux où ils se sont un peu plus éloignés.
def correlateByAvgDistanceFromRefPointWithLimit(df1, df2, limit, precision):
    reference_latitude = 48.89 
    reference_longitude = 2.34 
    print("filtering with limit")
    filteredDf1 = df1.filter((abs(df1.latitude - reference_latitude) > limit) & (abs(df1.longitude - reference_longitude) > limit))
    filteredDf2 = df2.filter((abs(df2.latitude - reference_latitude) > limit) & (abs(df2.longitude - reference_longitude) > limit))
    return correlateByAvgLocalisation(filteredDf1, filteredDf2, precision)    



def correlateByAvgLocalisation(df1, df2, precision):
    
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
        dataframe = dataframe.distinct()
        dataframe = dataframe.groupBy("week","idAno").agg(count("*").alias("count"), first("idOG").alias("idOG"))
        #Uniqdata = dataframe.where(col("count") == 1)
        UniqList.append(dataframe)

    print(">after selecting uniques list of values")
    merged_df = reduce(lambda df1, df2: df1.union(df2), UniqList)
    print(">after merging lists into one dataframe")
    merged_df = merged_df.groupBy("idOG","week","idAno").agg(count("*").alias("countVal"))
    merged_df = merged_df.groupBy("idOG","week","idAno").agg(max("countVal").alias("max_count"), first("countVal").alias("countVal"))
    merged_df = merged_df.where(col("countVal") == col("max_count"))
    print("> Keeping most probable value")
    merged_df = merged_df.dropDuplicates(["idAno","week"])
    #print(">keeping distinct values")
    return merged_df.select("idOG","week","idAno")

DataframeList = []
TestPrecision = [0.0001, 0.0003, 0.0005, 0.0007, 0.001, 0.003]
limitList = [5,0.1,0.005,0.001]
for p in TestPrecision:
    DataframeList.append(correlateByAvgLocalisation(df1, df2, p))
    for l in limitList:
        limDf = correlateByAvgDistanceFromRefPointWithLimit(df1,df2,l,p)
        DataframeList.append(limDf)
    

print("before first merging")
merged = merge_dataframes(DataframeList)



print(">before writing...")
merged.withColumnRenamed("idOG", "ID")    # 3 colonnes : ID, Date, ID_Anon
merged.withColumnRenamed("week", "Date")
merged.withColumnRenamed("idAno", "ID_Anon")
mergedpd = merged.toPandas()
idlisttab = df1.select("idOG").distinct()
idlisttab = idlisttab.toPandas().values.tolist()
idlist = []
for id in idlisttab:
    idlist.append(id[0])
json_out = tojson.dataframeToJSON(mergedpd,True, idlist)
with open("identifiedautofill.json", "w") as outfile:
    outfile.write(json_out)
#CSVManager.writeTabCSVFile(merged.toPandas(),"MergedcorrelationByAvg")
