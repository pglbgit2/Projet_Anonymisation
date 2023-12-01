from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def suppr(path, to, values_to_keep):
    spark = SparkSession.builder.appName("Suppression").config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
    df = spark.read.csv(path, header=True, sep='\t')
    df = df.withColumn("random", F.rand())
    window_spec = Window.partitionBy("id", "week", "type").orderBy("random")
    df = df.withColumn("rank", F.row_number().over(window_spec))
    colonnes_a_mettre_a_jour = ["anonymId", "timestamp", "new_longitude", "new_latitude"]
    for colonne in colonnes_a_mettre_a_jour:
        df = df.withColumn(colonne, F.when(F.col("rank") <= values_to_keep, F.col(colonne)).otherwise("DEL"))
    df = df.drop("random", "rank")
    df = df.withColumn("numero_ligne", F.col("numero_ligne").cast("bigint"))
    df =df.sort("numero_ligne", ascending=[True])
    df.show()
    # df = df.drop("id", "week", "longitude", "latitude", "numero_ligne")
    df_fin = df.select("anonymId", "timestamp", "new_longitude", "new_latitude")
    df_fin.coalesce(1).write.csv(to, header=False, mode="overwrite", sep="\t")



if __name__=="__main__":
    suppr("anonymFrangipane.csv/part-00000-16ef9d57-f950-4001-8758-c33e223c9954-c000.csv", "final.csv", 800)
