from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.functions import filter
import itertools

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
    return spark.read.csv(path, header=False, schema=schema, sep='\t')

# on prend deux points qui servent de reference pour le tracé: ils délimitent les points en haut à gauche et en bas à droite d'un rectangle
def Quadrillage(x1,y1,x2,y2,precision=0.05):
    nx = round(abs(x2-x1)/precision)
    tabx = []
    x = round(min([x1,x2]),round(1/precision))
    for i in range(nx):
        tabx.append(x+i*precision)
    ny = round(abs(y2-y1)/precision)
    taby = []
    y = round(min([y1,y2]),round(1/precision))
    for j in range(ny):
        taby.append(y+j*precision)
    return (tabx,taby)

def round_quadrillage(row, tab): # row: [longitude / latitude] selon ce qu'on choisit
    return closest_without_border(row[0], tab)

def closest_without_border(v,tab):
    if min(tab)<=v<=max(tab):
        return min(tab, key=lambda x: abs(x-v))
    else:
        return -1

def get_dict_from_df(df, tabx, taby):
    # df = df.withColumn("timestamp", F.round((F.round(F.unix_timestamp("timestamp")/60)/20)*1200).cast("timestamp"))
    df = df.withColumn("timestamp", F.expr("substring(timestamp, 1, length(timestamp)-4) || '0:00'"))

    print(">rounding timestamp to 10 minutes gap")
    round_longitude_udf = F.udf(lambda z: round_quadrillage(z, taby), DoubleType())
    round_latitude_udf = F.udf(lambda z: round_quadrillage(z, tabx), DoubleType())

    df = df.withColumn('longitude', round_longitude_udf(F.array('longitude')))
    # df.show(5)

    df = df.filter(F.col("longitude") != -1)
    # df.show(5)

    df = df.withColumn('latitude', round_latitude_udf(F.array('latitude')))
    # df.show(5)

    df = df.filter(F.col("latitude") != -1)
    # df.show(5)

    df = df.groupBy("timestamp","longitude","latitude").agg(F.count("*").alias("nbValue"))
    # df.show(5)

    df = df.dropDuplicates()
    # df.show(5)
    print(">after count")
    results = {} # dictionnaire par position
    for row in df.collect():
        if (row[1],row[2]) not in results.keys():
            results[(row[1],row[2])] = dict()
        results[(row[1],row[2])][row[0]] = row[3]
    print(">after putting in dictionnary")
    return results

def metric_compare_dict(dicAnon,dicOrigin):
    # dic be like : results[position]-> {timestamp:nbValue}
    for resultA in dicAnon:
        pass
    for resultO in dicOrigin:
        pass

if __name__ == '__main__':
    (tabx, taby) = Quadrillage(45.850, 4.730, 45.623, 5.020)
    ogdf = readfile("../default.csv")
    print(">after read original file")
    og_dict = get_dict_from_df(ogdf,tabx,taby)
    #print(og_dict)
    # andf = readfile("../Anon.csv")
    # print(">after read anonymise file")
    # an_dict = get_dict_from_df(andf,tabx,taby)



    

    