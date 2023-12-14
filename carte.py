import matplotlib.pyplot as plt
#pip3 install cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, collect_list, lit, when, date_trunc, avg, hour, rand, rand, randn, col, weekofyear, monotonically_increasing_id, expr, row_number, dayofweek, explode, from_unixtime, unix_timestamp, round, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

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

def plot_points(df):
    # Convertir le DataFrame Spark en DataFrame pandas pour le traçage
    pandas_df = df.toPandas()

    # Créer une figure et un axe
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})

    # Ajouter les caractéristiques géographiques
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    ax.add_feature(cfeature.RIVERS)

    # Tracer les points
    ax.scatter(pandas_df['avg(longitude)'], pandas_df['avg(latitude)'])

    # Afficher la figure
    plt.show()

if __name__ == '__main__':
    df, spark = readfile("../default.csv")
    conditionHome=((hour(df["timestamp"]) >= 22) | (hour(df["timestamp"]) < 6)) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6))
    conditionWork=(hour(df["timestamp"]) >= 9) & (hour(df["timestamp"]) < 16) & ((dayofweek(df["timestamp"]) >= 2) & (dayofweek(df["timestamp"]) <= 6))
    ConditionWeekend=(hour(df["timestamp"]) >= 10) & (hour(df["timestamp"]) < 18) & ((dayofweek(df["timestamp"]) < 2) | (dayofweek(df["timestamp"]) > 6))
    dfHome = df.filter(conditionHome)  # Jours du lundi au jeudi (2=lundi selon ChatGPT)
    dfWork = df.filter(conditionWork)
    dfWeekend = df.filter(ConditionWeekend)
    # df = df.withColumn("type", when((conditionHome|conditionWork|ConditionWeekend), col("type")).otherwise("del"))

    # pour tous les ids calculer des différents points of interest
    dfHome_average = dfHome.groupBy("id").avg("latitude", "longitude")
    dfWork_average = dfWork.groupBy("id").avg("latitude", "longitude")
    dfWeekend_average = dfWeekend.groupBy("id").avg("latitude", "longitude")
    plot_points(dfHome_average)
