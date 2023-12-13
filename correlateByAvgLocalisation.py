from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, weekofyear, abs, col, count, first, max, dayofweek, hour, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import tojson
spark = SparkSession.builder.appName("CalculatingAveragePosition").getOrCreate()


def readfile(path: str):
    spark = SparkSession.builder.appName("CalculatingDayNightPosition")\
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True)
    ])
    print(">after schema definition")
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("dayOfWeek", dayofweek("timestamp")).withColumn("week",  weekofyear("timestamp"))


# selectionne d'abord uniquement les points s'éloignant plus qu'une certaine distance limite avant de corréler par moyenne
# l'objectif est de se débarasser des points souvent fréquenté par tout le monde et de sélectionner uniquement ceux où ils se sont un peu plus éloignés.
def correlateByAvgDistanceFromRefPointWithLimit(df1, df2, limit, precision):
    reference_latitude = 48.89 
    reference_longitude = 2.34 
    print("filtering with limit")
    filteredDf1 = df1.filter((abs(df1.latitude - reference_latitude) > limit) & (abs(df1.longitude - reference_longitude) > limit))
    filteredDf2 = df2.filter((abs(df2.latitude - reference_latitude) > limit) & (abs(df2.longitude - reference_longitude) > limit))
    return correlateByAvgLocalisation(filteredDf1, filteredDf2, precision, 9, 16, 22, 6)    

def moyenneParIdParSemaine(df, nomidentifiant):
    return df.groupBy(nomidentifiant, "week").agg(avg("longitude").alias("avg_longitude"), avg("latitude").alias("avg_latitude"))


def mergeDfMoyenne(df1, df2, seuil):
    return df1.join(df2, "week", "inner").filter((abs(df1.avg_longitude - df2.avg_longitude) <= seuil) & (abs(df1.avg_latitude - df2.avg_latitude) <= seuil))

def correlateByAvgLocalisation(startOfTheWork, endOfTheWork, startOfTheNight, endOfTheNight, OriginalDf, anonymDf, precision):
    
    # Calculer la position moyenne par semaine dans chaque DataFrame
    conditionAnonymWork = ((1 < dayofweek(anonymDf["timestamp"])) & (dayofweek(anonymDf["timestamp"]) <= 6) &((hour(anonymDf["timestamp"]) >= startOfTheWork) & (hour(anonymDf["timestamp"]) < endOfTheWork)))
    conditionAnonymNight = ((1 < dayofweek(anonymDf["timestamp"])) & (dayofweek(anonymDf["timestamp"]) <= 6) &((hour(anonymDf["timestamp"]) < endOfTheNight) | (hour(anonymDf["timestamp"]) >= startOfTheNight)))
    conditionAnonymWeekend = (((dayofweek(anonymDf["timestamp"]) == 1) | (dayofweek(anonymDf["timestamp"]) == 7)) & ((10 < hour(anonymDf["timestamp"])) & (hour(anonymDf["timestamp"]) < 18)))
    conditionAnonymOther = ~ (conditionAnonymWork | conditionAnonymNight | conditionAnonymWeekend)

    AnonymDfWork = anonymDf.filter(conditionAnonymWork)
    AnonymDfNight = anonymDf.filter(conditionAnonymNight)
    AnonymDfWeekend = anonymDf.filter(conditionAnonymWeekend)
    AnonymDfOther = anonymDf.filter(conditionAnonymOther)

    AnonymDfWork = AnonymDfWork.withColumn("type", lit("work"))
    AnonymDfNight = AnonymDfNight.withColumn("type", lit("night"))
    AnonymDfWeekend = AnonymDfWeekend.withColumn("type", lit("we"))
    AnonymDfOther = AnonymDfOther.withColumn("type", lit("oth"))

    conditionOriginalWork = ((1 < dayofweek(OriginalDf["timestamp"])) & (dayofweek(OriginalDf["timestamp"]) <= 6) &((hour(OriginalDf["timestamp"]) >= startOfTheWork) & (hour(OriginalDf["timestamp"]) < endOfTheWork)))
    conditionOriginalNight = ((1 < dayofweek(OriginalDf["timestamp"])) & (dayofweek(OriginalDf["timestamp"]) <= 6) &((hour(OriginalDf["timestamp"]) < endOfTheNight) | (hour(OriginalDf["timestamp"]) >= startOfTheNight)))
    conditionOriginalWeekend = (((dayofweek(OriginalDf["timestamp"]) == 1) | (dayofweek(OriginalDf["timestamp"]) == 7)) & ((10 < hour(OriginalDf["timestamp"])) & (hour(OriginalDf["timestamp"]) < 18)))
    conditionOriginalOther = ~ (conditionOriginalWork | conditionOriginalNight | conditionOriginalWeekend)

    OriginalDfWork = OriginalDf.filter(conditionOriginalWork)
    OriginalDfNight = OriginalDf.filter(conditionOriginalNight)
    OriginalDfWeekend = OriginalDf.filter(conditionOriginalWeekend)
    OriginalDfOther = OriginalDf.filter(conditionOriginalOther)

    OriginalDfWork = OriginalDfWork.withColumn("type", lit("work"))
    OriginalDfNight = OriginalDfNight.withColumn("type", lit("night"))
    OriginalDfWeekend = OriginalDfWeekend.withColumn("type", lit("we"))
    OriginalDfOther = OriginalDfOther.withColumn("type", lit("oth"))

    print("> After day, night and weekend dataframes")

    AnonymAvgWork = moyenneParIdParSemaine(AnonymDfWork, "idAno")
    AnonymAvgNight = moyenneParIdParSemaine(AnonymDfNight, "idAno")
    AnonymAvgWeekend = moyenneParIdParSemaine(AnonymDfWeekend, "idAno")
    AnonymAvgOthers = moyenneParIdParSemaine(AnonymDfOther, "idAno")

    OriginalAvgWork = moyenneParIdParSemaine(OriginalDfWork, "idOG")
    OriginalAvgNight = moyenneParIdParSemaine(OriginalDfNight, "idOG")
    OriginalAvgWeekend = moyenneParIdParSemaine(OriginalDfWeekend, "idOG")
    OriginalAvgOthers = moyenneParIdParSemaine(OriginalDfOther, "idOG")

    print(">after moyenne")

    AvgWork = mergeDfMoyenne(AnonymAvgWork, OriginalAvgWork, precision)
    AvgNight = mergeDfMoyenne(AnonymAvgNight, OriginalAvgNight, precision)
    AvgWeekend = mergeDfMoyenne(AnonymAvgWeekend, OriginalAvgWeekend, precision)
    AvgOthers = mergeDfMoyenne(AnonymAvgOthers, OriginalAvgOthers, precision)

    print("after merging on day, night, weekend, other")

    return merge_df([AvgWork, AvgNight, AvgWeekend, AvgOthers])

#Version raccourcie, suppose que deja unique
def merge_df(dataFrameList):
    merged_df = reduce(lambda df1, df2: df1.union(df2), dataFrameList)
    merged_df = merged_df.groupBy("idOG","week","idAno").agg(count("*").alias("countVal"))
    merged_df = merged_df.groupBy("idOG","week","idAno").agg(max("countVal").alias("max_count"), first("countVal").alias("countVal"))
    merged_df = merged_df.where(col("countVal") == col("max_count"))
    merged_df = merged_df.dropDuplicates(["idAno","week"])
    return merged_df.select("idOG","week","idAno")

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



def attackfile(filename):
    Origindf = readfile("ReferenceINSA.csv")
    print(">after reading original file")
    Anonymdf = readfile(filename)
    print(">after reading anonymized file")
    Origindf = Origindf.withColumnRenamed("id", "idOG")
    Anonymdf = Anonymdf.withColumnRenamed("id", "idAno")
    print(">after re-naming columns")

    DataframeList = []
    TestPrecision = [0.0001, 0.0003, 0.0005, 0.0007, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3]
    for p in TestPrecision:
        DataframeList.append(correlateByAvgLocalisation(9, 16, 22, 6, Origindf, Anonymdf, p))
        
    print("before first merging")
    merged = merge_dataframes(DataframeList)

    print(">before writing...")
    merged.withColumnRenamed("idOG", "ID")    # 3 colonnes : ID, Date, ID_Anon
    merged.withColumnRenamed("week", "Date")
    merged.withColumnRenamed("idAno", "ID_Anon")
    mergedpd = merged.toPandas()
    idlisttab = Origindf.select("idOG").distinct()
    idlisttab = idlisttab.toPandas().values.tolist()
    idlist = []
    for id in idlisttab:
        idlist.append(id[0])
    json_out = tojson.dataframeToJSON(mergedpd,True, idlist)
    with open("identified701_2.json", "w") as outfile:
        outfile.write(json_out)
attackfile("files/VinAnonyme_701_clean.csv")