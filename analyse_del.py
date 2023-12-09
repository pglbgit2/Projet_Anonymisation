import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, collect_list, lit, when, date_trunc, avg, hour, rand, rand, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode, from_unixtime, unix_timestamp, round, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas as pd
from itertools import tee


def readfile(path: str):
    spark = SparkSession.builder.appName("loader")\
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
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id()), spark

def analyse_HeureCoordonnée(df):
    # Filtrer le DataFrame pour ne contenir que les lignes où l'ID est "DEL"
    df_del = df.filter(df.id == "DEL")

    # Regrouper par heure et compter le nombre de fois où chaque heure apparaît
    df_del_hour = df_del.groupBy(hour(df_del.timestamp).alias('hour')).count().orderBy('count', ascending=False)

    # Regrouper par coordonnées GPS et compter le nombre de fois où chaque coordonnée apparaît
    df_del_coords = df_del.groupBy('longitude', 'latitude').count().orderBy('count', ascending=False)

    # Afficher les résultats
    df_del_hour.show()
    df_del_coords.show()


if __name__ == '__main__':
    df, spark = readfile("victime.csv")
    analyse_HeureCoordonnée(df)
