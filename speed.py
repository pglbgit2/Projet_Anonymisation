from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import count, udf, array, lead, col, unix_timestamp, hour
import itertools
from quadrillage import round_quadrillage


def del_lignes(df):
    df = df.where(df.id != 'DEL')
    df = df.orderBy("id", "timestamp")


def prep_vitesse(df, heure_depart, heure_arrivee, heure_retour_depart, heure_retour_arrivee):
    df = df.withColumn("next_longitude", lead("longitude"))
    df = df.withColumn("next_latitude", lead("latitude"))
    df = df.withColumn("cat", "matin").where(hour(df.timestamp)>=heure_depart & (df.timestamp)<=heure_arrivee)
    df = df.withColumn("cat", "soir").where(hour(df.timestamp)>=heure_retour_depart & hour(df.timestamp)<=heure_retour_arrivee)
    df = df.where(df.cat=="matin" | df.cat =="soir")
    df.withColumn("distance", ((df.next_latitude-df.latitude)**2 - (df.next_longitude-df.longitude)**2)**(1/2))

    df_sum = df.groupBy("id", "day", "cat").aggr(max("timestamp").alias("max_temps"), min("timestamp").alias("min_temps"),sum("distance").alias("distance_total"))
    df_spe = df_sum.withColumn("speed",df_sum.distance_totale/(df.max_temps-df.min_temps))
    return df_spe

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
    df_anon = readfile("files/???.csv")
    del_lignes(df_anon)
    df_ori_speed = prep_vitesse(df_ori, 7, 8, 17, 19)
    df_anon_speed = prep_vitesse(df_ori, 7, 8, 17, 19)