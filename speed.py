from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import count, udf, array, lead
import itertools

def prep_vitesse(df):
    df = df.withColumn("next_longitude", lead("longitude"))
    df = df.withColumn("next_latitude", lead("latitude"))
    df = df.withColumn("next_timestamp", lead("timestamp"))

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
    df_anon = df_anon.orderBy("id", "timestamp")
