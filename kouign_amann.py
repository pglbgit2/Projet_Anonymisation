import string
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, randn, col, weekofyear, monotonically_increasing_id, expr, row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import CSVManager
import pandas as pd

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
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id())
    

def beurre(df):
    # TODO: Fixer la pseudonimization pour un couple (id; week)
    dfHome = df.filter((hour(df["timestamp"]) >= 22) & (hour(df["timestamp"]) < 6) & (df['timestamp'].cast('date').cast('timestamp').cast('int').cast('timestamp') <= df["timestamp"]))
    dfWork = df.filter((hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & (df['timestamp'].cast('date').cast('timestamp').cast('int').cast('timestamp') <= df["timestamp"]))
    dfWeekend = df.filter((hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & (df['timestamp'].cast('date').cast('timestamp').cast('int').cast('timestamp') <= df["timestamp"]))

    # pour tous les ids calculer des différents points of interest
    dfHome_average = dfHome.groupBy("id").avg("latitude", "longitude")
    dfWork_average = dfWork.groupBy("id").avg("latitude", "longitude")
    dfWeekend_average = dfWeekend.groupBy("id").avg("latitude", "longitude")
    dfWeekend_average.show()
        
        


if __name__ == '__main__':
    df = readfile("res.csv")
    df.show()
    beurre(df)