import csv
import pandas as pd
import numpy as np
import json
import CSVmanager


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
            with open(Anon_file+"_Identification.json", "w") as outfile:
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

    # Ne garder que la date Année-Mois
    print(">> Ignorez l'alerte ci-dessous")
    liste_date = df_merge['Date']
    for i in range(0, len(liste_date)):
        liste_date[i] = liste_date[i][:7]
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
        iden_Anon = Liste_finale[i][2]

        if not isKey(json_rendu, identifiant):
            json_rendu[identifiant] = dict()

        json_rendu[identifiant][date] = iden_Anon

    json_out = json.dumps(json_rendu)
    with open(Anon_file[:-4]+"_Identification.json", "w") as outfile:
        outfile.write(json_out)

    # PROFIT !


# sort_table("autofill_476_clean.csv","2015-03-27 13:13:55") #* K E E P    O U T *
# identification("autofill_476_clean.csv", "ReferenceINSA.csv")
identificationV2("TresAnonyme_428_clean.csv", "ReferenceINSA.csv")
