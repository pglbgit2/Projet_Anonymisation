from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import count, udf, array, lead, col, unix_timestamp, hour, dayofweek, max, min, sum, avg, window, abs, radians, cos, sin, asin, sqrt
import itertools


def del_lignes(df):
    df = df.where(df.id != 'DEL')
    # On supprime les lignes du week-end
    df = df.filter(dayofweek(df.timestamp).isin([2, 6]))
    # On garde que les heures de trajet
    conditionTrajet=(((hour(df["timestamp"]) < 22) & (hour(df["timestamp"]) >= 16)) | ((hour(df["timestamp"]) >= 6) & (hour(df["timestamp"]) < 9)))
    df = df.filter(conditionTrajet)
    df = df.orderBy("id", "timestamp")
    return df

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in meters between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000


def prep_vitesse(df):
    windowSpec = Window.partitionBy("id").orderBy("timestamp")
    # Gestion des doublons de timestamp
    df = df.groupBy("id", "timestamp")\
        .agg(avg("longitude").alias("longitude"), 
             avg("latitude").alias("latitude"))
    
    # Ajout de next longitude et latitude
    df = df.withColumn("next_longitude", lead("longitude").over(windowSpec))
    df = df.withColumn("next_latitude", lead("latitude").over(windowSpec))

    # Création des groupes de 10 minutes
    df = df.withColumn("time_window", window(df.timestamp, "10 minutes"))

    # Calcule de la distance parcourue entre deux points
    df = df.withColumn("diff_longitude", abs(col("next_longitude") - col("longitude")))
    df = df.withColumn("diff_latitude", abs(col("next_latitude") - col("latitude")))
    # Conversion en metres
    df = df.withColumn("distance", haversine(col("longitude"), col("latitude"), col("next_longitude"), col("next_latitude")))
    df = df.drop("diff_longitude", "diff_latitude", "next_longitude", "next_latitude", "longitude", "latitude")
    # Somme des distances parcourues par groupe de 10 minutes
    df = df.groupBy("id", "time_window").agg(sum("distance").alias("total_distance"))
    # Calcul de la vitesse en km/h
    df = df.withColumn("vitesse", (col("total_distance") * 0.006))
    # On supprime les vitesses autres que la marche
    df = df.filter(df.vitesse > 3).filter(df.vitesse < 13)

    df = df.orderBy("id").groupBy("id").sum("total_distance")
    df = df.groupBy().avg("sum(total_distance)")

    df.show(100)

    # Il y a t'il plus opti ?????Car c'est long pour peu
    result = df.first()[0]
    final_value = int(result)
    print(final_value)
    return final_value

def readfile(path: str):
    spark = SparkSession.builder.appName("CalculatingSpeed")\
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

if __name__ == '__main__':
    df_ori = readfile("ReferenceINSA.csv")
    df_anon = readfile("alo.csv")
    df_ori = del_lignes(df_ori)
    df_anon = del_lignes(df_anon)
    distanceMoyenneMarcheOri = prep_vitesse(df_ori)
    distanceMoyenneMarcheAnon = prep_vitesse(df_anon)
    print(distanceMoyenneMarcheAnon/distanceMoyenneMarcheOri)