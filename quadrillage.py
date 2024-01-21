from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F
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
def Quadrillage(x1,y1,x2,y2,precision=0.1):
    nx = round(abs(x2-x1)/precision)
    tabx = []
    x = min([x1,x2])
    for i in range(nx):
        tabx.append(x+i*precision)
    ny = round(abs(y2-y1)/precision)
    taby = []
    y = min([y1,y2])
    for j in range(ny):
        taby.append(y+j*precision)
    return (tabx,taby)

def round_quadrillage(row, tab): # row: [longitude / latitude] selon ce qu'on choisit
    return closest_without_border(row[0], tab)

def closest_without_border(v,tab):
    if min(tab)<=v<=max(tab):
        min(tab, key=lambda x: abs(x-v))
    else:
        return -1


if __name__ == '__main__':
    df = readfile("../ReferenceINSA.csv")
    print(">after read original file")
    (tabx, taby) = Quadrillage(45.850, 4.730, 45.623 , 5.020)
    df = df.withColumn("timestamp", F.round((F.unix_timestamp("timestamp")/1200)*1200).cast("timestamp"))
    print(">rounding timestamp to 20 minutes gap")
    round_longitude_udf = F.udf(lambda z: round_quadrillage(z, taby), DoubleType())
    round_latitude_udf = F.udf(lambda z: round_quadrillage(z, tabx), DoubleType())
    df = df.withColumn('longitude', round_longitude_udf(F.array('longitude')))
    df = df.filter(F.col("longitude") != -1)
    df = df.withColumn('latitude', round_latitude_udf(F.array('latitude')))
    df = df.filter(F.col("latitude") != -1)

    df = df.groupBy("timestamp","longitude","latitude").agg(F.count("*").alias("nbValue"))
    df = df.dropDuplicates()
    print(">after count")        
    results = {} # dictionnaire par position
    for position in itertools.product(tabx,taby):
        results[position] = {row['timestamp']:row['nbValue'] for row in df.collect()}
    print(">after putting in dictionnary")        



    

    