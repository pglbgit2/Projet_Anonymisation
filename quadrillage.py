from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import count, udf, array
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
def Quadrillage(x1,y1,x2,y2,precision=0.001):
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
    return min(tab, key=lambda x:abs(x-row[0]))


if __name__ == '__main__':
    df = readfile("../ReferenceINSA.csv")
    print(">after read original file")
    (tabx, taby) = Quadrillage(51.016, -4.794, 42.483 , 8.117)
    round_longitude_udf = udf(lambda z: round_quadrillage(z, taby), DoubleType())
    round_latitude_udf = udf(lambda z: round_quadrillage(z, tabx), DoubleType())
    df = df.withColumn('longitude', round_longitude_udf(array('longitude')))
    df = df.withColumn('latitude', round_latitude_udf(array('latitude')))
    df = df.groupBy("timestamp","longitude","latitude").agg(count("*").alias("nbValue"))
    df = df.dropDuplicates()
    print(">after count")        
    results = {} # dictionnaire par position
    for position in itertools.product(tabx,taby):
        results[position] = {row['timestamp']:row['nbValue'] for row in df.collect()}
    print(">after putting in dictionnary")        



    

    