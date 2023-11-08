import csv
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import pyspark.sql as psk
from datetime import datetime, date
import json
import CSVmanager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from utils import semaine2015


def correlate(value1, value2):
    assert len(value1) == len(value2)
    nb = 0
    nb = sum(1 for v1, v2 in zip(value1, value2) if v1 == v2)
    return nb


def isKey(Dict, Key):
    return Key in Dict.keys()


def equal(element1, element2, limite):
    assert limite >= 0
    assert len(element1) >= limite
    assert len(element2) >= limite
    match = 0
    for i in range(0, limite, 1):
        if element1[i] == element2[i]:
            match += 1
    return match == limite


def sort_table(Table_csv, byData):
    data = pd.read_csv(Table_csv, sep="\t", names=['ID', 'Date', 'Long', 'Lat'], header=None)
    a = data.sort_values(by=[byData], axis=0)
    a.to_csv(Table_csv, sep="\t")


def identification(Anon_file, Original_file):
    # Algo très long à l'éxecution. Opère sur des descripteurs de fichiers uniquement

    with open(Anon_file, 'r') as Anon:
        with open(Original_file, 'r') as BDD:
            readerBDD = csv.reader(BDD, delimiter='\n')
            readerAnon = csv.reader(Anon, delimiter='\n')

            json_rendu = dict()
            mois_parcouru = dict()

            for row in readerBDD:
                line = str(row)
                line = line[2:-2]
                data = line.split("\\t")
                identifiant = int(data[0])
                date = str(data[1])
                date_du_mois = date[0:7]
                long = float(data[2])
                lat = float(data[3])

                if not isKey(json_rendu, identifiant):
                    json_rendu[identifiant] = dict()
                    mois_parcouru[identifiant] = set()
                    print("Nouveau identifiant")

                if date_du_mois not in mois_parcouru[identifiant]:
                    # print("Nouvelle Boucle interne")
                    for rows in readerAnon:
                        line_Anon = str(rows)
                        line_Anon = line_Anon[2:-2]
                        data_Anon = line_Anon.split("\\t")
                        if date == data_Anon[1] and \
                                float(data_Anon[2]) - 0.1 <= long <= float(data_Anon[2]) + 0.1 and \
                                float(data_Anon[3]) - 0.1 <= lat <= float(data_Anon[3]) + 0.1:
                            print(data)
                            print(data_Anon)
                            print("Correspondance trouvée")
                            mois_parcouru[identifiant].add(date_du_mois)
                            json_rendu[identifiant][date_du_mois] = data_Anon[0]
                            break
                    Anon.seek(0)  # On se remet au début du fichier
                else:
                    continue

            json_out = json.dumps(json_rendu)
            with open(Anon_file + "_Identification.json", "w") as outfile:
                outfile.write(json_out)


def identificationV2(Anon_file, Original_file):
    # Cette algorithme à été conçu pour réidentifier la base de Données autofill_476.csv,
    # une fois ses lignes 'DEL' supprimées. (Voir suppr.py sur le discord)

    print("> Chargement des tables CSV dans les Data-Frames Pandas.")
    data_Origin = pd.read_csv(Original_file, sep='\t', names=['ID', 'Date', 'Long', 'Lat'], header=None)
    data_Anon = pd.read_csv(Anon_file, sep='\t', names=['ID_Anon', 'Date', 'Long', 'Lat'], header=None)

    print(Original_file + ":")
    data_Origin.info(verbose=True)
    print("\n ----- \n")
    print(Anon_file + ":")
    data_Anon.info(verbose=True)

    print("\n> Chargement fini.")
    print("\n> Début du processus de réidentification :\n")

    json_rendu = dict()

    # Arrondir Long et Lat du Origin au centième.
    # print(data_Origin.head(8))
    print("> Arrondissement des coordonnées géographiques.")
    data_Origin['Long'] = np.round(data_Origin['Long'], decimals=2)
    data_Origin['Lat'] = np.round(data_Origin['Lat'], decimals=2)
    # print(data_Origin.head(8))

    print("> Jointure interne des Data-Frames Origin et Anon. (Cela peut prendre un petit moment)")
    df_merge = data_Origin.merge(data_Anon, on=['Date', 'Long', 'Lat'], how='inner')
    # df_merge.info(verbose=True)

    # Retirer Long et Lat

    print("> Suppression des colonnes/éléments indésirables.")
    df_merge.drop(['Long', 'Lat'], axis=1, inplace=True)



    # Ne garder que la date Année-Mois (Transformer la date du jour en Année-Numéro_Semaine)
    print(">> Ignorez l'alerte ci-dessous")
    liste_date = df_merge['Date']
    print("Longueur de la liste Liste-Date : "+str(len(liste_date)))
    for i in range(0, len(liste_date)):
        Date_du_jour = liste_date[i][5:10]
        for j in range(10, 20, 1):
            if Date_du_jour in semaine2015[str(j)]:
                liste_date[i] = "2015-"+str(j)
                break

    df_merge['Date'] = liste_date

    # print(df_merge.head(6))

    # Supprimer les doublons
    df_merge.drop_duplicates(keep='first', inplace=True)
    df_merge.info(verbose=True)

    # ???

    print("> Récupération des informations et mise sous format JSON")

    df_merge.sort_values(by=['ID'], axis=0)
    Liste_finale = df_merge.values.tolist()

    for i in range(0, len(df_merge['ID']) - 1):

        identifiant = Liste_finale[i][0]
        date = Liste_finale[i][1]
        iden_Anon = [str(Liste_finale[i][2])]

        if not isKey(json_rendu, identifiant):
            json_rendu[identifiant] = dict()

        json_rendu[identifiant][date] = iden_Anon

    json_out = json.dumps(json_rendu)
    with open(Anon_file[:-4] + "_Identification.json", "w") as outfile:
        outfile.write(json_out)

    # PROFIT !


def identificationV3(Anon_file, Original_file):
    # à Executer avec les droits administrateurs

    spark = SparkSession.builder.appName("DistanceCalculations").getOrCreate()

    schemaOriginal = StructType([
        StructField("identifiant", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True)
    ])

    schemaAnon = StructType([
        StructField("identifiant_Anon", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True)
    ])

    df_Origin = spark.read.csv(Original_file, sep="\t", header=False, schema=schemaOriginal)
    df_Anon = spark.read.csv(Anon_file, sep="\t", header=False, schema=schemaAnon)

    # Définissez la position géographique de référence pour chaque groupe
    reference_latitude = 45.764043  # Remplacez par votre latitude de référence
    reference_longitude = 4.835659  # Remplacez par votre longitude de référence

    # Ici la référence est le centre de Lyon

    df_Origin = df_Origin.withColumn("distance_to_reference",
                                     expr("SQRT(POW(latitude - {0}, 2) + POW(longitude - {1}, 2))".format(
                                         reference_latitude,
                                         reference_longitude))
                                     )

    df_Anon = df_Anon.withColumn("distance_to_reference",
                                 expr("SQRT(POW(latitude - {0}, 2) + POW(longitude - {1}, 2))".format(
                                     reference_latitude,
                                     reference_longitude))
                                 )

    # On ne garde que le mois pour traiter les distances
    # df_Anon.withColumn("timestamp",col("timestamp").cast(StringType()))
    df_Anon = df_Anon.withColumn("timestamp", col("timestamp").substr(0, 7))

    # df_Origin.withColumn("timestamp", col("timestamp").cast(StringType()))
    df_Origin = df_Origin.withColumn("timestamp", col("timestamp").substr(0, 7))

    print(df_Origin.head(1))

    # Sélectionnez la ligne avec la distance maximale pour chaque identifiant : Couplé avec la date du mois !
    df_Origin = df_Origin.groupBy(["identifiant", "timestamp"]).max("distance_to_reference")
    df_Origin = df_Origin.withColumnRenamed("max(distance_to_reference)", "max_distance")

    print("Nombre de couples Identifiants-Mois-DistanceMax dans Origin : " + str(df_Origin.count()))

    df_Anon = df_Anon.groupBy(["identifiant_Anon", "timestamp"]).max("distance_to_reference")
    df_Anon = df_Anon.withColumnRenamed("max(distance_to_reference)", "max_distance")

    print("Nombre de couples Identifiants-Mois-DistanceMax dans df_Anon : " + str(df_Anon.count()))

    # Ici le code pour arrondir les distances si jamais ça ne marche pas

    precision = 2
    df_Anon = df_Anon.withColumn("max_distance", round("max_distance", precision))
    df_Origin = df_Origin.withColumn("max_distance", round("max_distance", precision))

    df_Anon = df_Anon.toPandas()
    df_Origin = df_Origin.toPandas()

    df_Anon.to_csv("tmp_Anon.csv")
    df_Origin.to_csv("tmp_Origin.csv")

    # Technique d'utilisateur windows + repompage QUALI

    json_rendu = dict()

    with open("tmp_Anon.csv", 'r') as Anon:
        with open("tmp_Origin.csv", 'r') as BDD:
            readerBDD = csv.reader(BDD, delimiter='\n')
            readerAnon = csv.reader(Anon, delimiter='\n')

            for rowA in readerAnon:
                line = str(rowA)
                line = line[2:-2]
                if line[0] == ',':
                    continue
                data_Anon = line.split(",")
                ID_Anon = data_Anon[1]
                Date_Anon = data_Anon[2]
                DistMax_Anon = float(data_Anon[3])

                for rowO in readerBDD:
                    line = str(rowO)
                    line = line[2:-2]
                    if line[0] == ',':
                        continue
                    data = line.split(",")
                    ID_Origin = data[1]
                    Date_Origin = data[2]
                    DistMax_Origin = float(data[3])

                    if not isKey(json_rendu, ID_Origin):
                        json_rendu[ID_Origin] = dict()

                    if DistMax_Anon - 1 * 10 ** (precision - 1) <= DistMax_Origin <= DistMax_Anon + 1 * 10 ** (
                            precision - 1) and \
                            Date_Anon == Date_Origin:
                        json_rendu[ID_Origin][Date_Origin] = ID_Anon
                        print("Correspondance")
                        print(ID_Origin + ":{" + Date_Origin + ":" + ID_Anon + "}")
                        break

                BDD.seek(0)

    json_out = json.dumps(json_rendu)
    with open(Anon_file[:-4] + "_Identification.json", "w") as outfile:
        outfile.write(json_out)

    spark.stop()

    # print("Merge en cours...")
    # df_merge = df_Origin.join(df_Anon, on=['timestamp', 'max_distance'], how='inner')
    # print("Nombre de couples Identifiants-Mois-DistanceMax dans df_merge : " + str(df_merge.count()))
    # print(df_merge.head(4))


# sort_table("autofill_476_clean.csv","2015-03-27 13:13:55") #* K E E P    O U T *
# identification("autofill_476_clean.csv", "ReferenceINSA.csv")
identificationV2("autofill_476_clean.csv", "ReferenceINSA.csv")
