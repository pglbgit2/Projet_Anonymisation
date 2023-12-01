import CSVManager
import os
import sys
import string
import random
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id, udf, array, weekofyear

def pseudonymize(array, idAno, generatedStrings):
    [id, week] = array
    if(id not in idAno):
        pseudo = ''
        while pseudo in generatedStrings:
            pseudo = generateString()
        idAno[id] = {}
        idAno[id][week] = pseudo
        return pseudo
    else:
        if(week not in idAno[id]):
            pseudo = ''
            while pseudo in generatedStrings:
                pseudo = generateString()
            idAno[id][week] = pseudo
            return pseudo
        else:
            return idAno[id][week]

def generateString(size=7):
    authorized_chars = string.ascii_letters + string.digits
    generated_string = ''.join(random.choice(authorized_chars) for _ in range(size))
    return generated_string

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
    return spark.read.csv(path, header=False, schema=schema, sep='\t').withColumn("numero_ligne", monotonically_increasing_id())

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Usage: python3 {} <source.csv> <dest.csv>\n".format(sys.argv[0]))
        sys.exit(1)

    df = readfile(sys.argv[1])
    df = df.withColumn('week', weekofyear(df['timestamp']))
    idPseudoDic = {}
    generatedStrings = set()
    generatedStrings.add('')
    pseudonymise_udf = udf(lambda z: pseudonymize(z, idPseudoDic, generatedStrings), StringType())
    anonymised = df.withColumn('anonymId', pseudonymise_udf(array('id', 'week')))
    anonymised = anonymised.withColumn('id', anonymised['anonymId']).drop('anonymId')  # Replace 'id' with 'anonymId' and drop 'anonymId'
    print(">after pseudonymisation")
    anonymised = anonymised.filter(df['id'].isNotNull())  # Filter out rows with null or empty 'id'
    anonymised = anonymised.drop('week', 'numero_ligne')  # Drop 'week' and 'numero_ligne' columns
    anonymised.coalesce(1).write.csv(sys.argv[2], header=False,  mode="overwrite", sep="\t")