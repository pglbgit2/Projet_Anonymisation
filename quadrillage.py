from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import count, udf

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

def extractLongitudeDfList(df, tab):
    dflist = []
    for i in range(len(tab)-1):
        dflist.append(df.filter((tab[i] <= df.longitude)&(df.longitude < tab[i+1])))
    return dflist

def extractLatitudeDfList(df, tab):
    dflist = []
    for i in range(len(tab)):
        filteredDf = df.filter((tab[i] <= df.latitude) & (df.latitude < tab[i+1]))
        filteredDf.groupBy("timestamp").agg(count("*").alias("nbValue"))
        dflist.append(filteredDf)
    return dflist

def fill_results(row, results,i,j):
    results[(tabx[i], taby[j])][row[1]] = row[4]

if __name__ == '__main__':
    df = readfile("../ReferenceINSA.csv")
    print(">after read original file")
    (tabx, taby) = Quadrillage(51.016, -4.794, 42.483 , 8.117)
    dflist = extractLongitudeDfList(df, tabx)
    print(">after splitting in line")
    results = {} # dictionnaire par position
    i = 0
    j = 0
    result_udf = udf(lambda z: fill_results(z, results))
    print(">udf fill results")
    for xline in dflist: # lignes
        xline.sort(xline.latitude)
        dfResultsList = extractLatitudeDfList(xline, taby)
        results[(tabx[i], taby[j])] = {} # dictionnaire par date
        j = 0
        for resultDf in dfResultsList: # colonnes
            for row in df.rdd.collect():
                fill_results(row,results,i,j) # [id,timestamp,longitude,latitude,nbValue]
            j+=1 
        print(">ligne "+str(i))
        i += 1
    
    