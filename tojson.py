import pandas as pd
import json

semainesUtils = {"2015-10", "2015-11", "2015-12", "2015-13", "2015-14", "2015-15", "2015-16", "2015-17", "2015-18",
                 "2015-19", "2015-20"}

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


def dataframeToJSON(Dataframe, keep_null=False, liste_id=None):
    # 3 colonnes : ID, Date, ID_Anon
    # Ligne type : 1, 15, 126

    Liste_finale = Dataframe.values.tolist()
    json_rendu = dict()

    for i in range(0, len(Liste_finale) - 1):

        identifiant = Liste_finale[i][0]
        date = "2015-" + str(Liste_finale[i][1])
        iden_Anon = [str(Liste_finale[i][2])]

        if not isKey(json_rendu, identifiant):
            json_rendu[identifiant] = dict()

        json_rendu[identifiant][date] = iden_Anon

    if keep_null:
        verif = {"2015-10": False, "2015-11": False, "2015-12": False, "2015-13": False, "2015-14": False,
                 "2015-15": False,
                 "2015-16": False, "2015-17": False, "2015-18": False,
                 "2015-19": False, "2015-20": False}

        for id in json_rendu.keys():
            for j in json_rendu[id].keys():
                if j in semainesUtils:
                    verif[j] = True

            for j in range(0, 9, 1):
                asso = verif.popitem()
                if asso[1] is False:
                    json_rendu[id][asso[0]] = None

            verif = {"2015-10": False, "2015-11": False, "2015-12": False, "2015-13": False, "2015-14": False,
                     "2015-15": False, "2015-16": False, "2015-17": False, "2015-18": False,
                     "2015-19": False, "2015-20": False}

        if liste_id is not None:
            # En fournissant une liste des identifiants originaux, ceux qui n'auront aucune correspondance avec Anon,
            # mais qui existe tout de même dans La base de données originelle, pourront apparaitre dans le json avec
            # des informations null : "2015-10": null, "2015-11": null ... etc

            for id in liste_id:
                if id not in json_rendu.keys():
                    for j in range(0,9,1):
                        date_2015 = verif.popitem()
                        print("id = " + id)
                        print("date = " + str(date_2015))
                        if id not in json_rendu:
                            json_rendu[id] = {}
                        json_rendu[id][date_2015[0]] = None

                    verif = {"2015-10": False, "2015-11": False, "2015-12": False, "2015-13": False, "2015-14": False,
                             "2015-15": False, "2015-16": False, "2015-17": False, "2015-18": False,
                             "2015-19": False, "2015-20": False}

    json_out = json.dumps(json_rendu, indent=4)

    return json_out

    # Après ça, on l'injecte dans le bon fichier !

    # with open(NOM_DU_FICHIER.json", "w") as outfile:
    #    outfile.write(json_out)


def sort_table(Table_csv, byData):
    data = pd.read_csv(Table_csv, sep="\t", names=['ID', 'Date', 'Long', 'Lat'], header=None)
    a = data.sort_values(by=[byData], axis=0)
    a.to_csv(Table_csv, sep="\t")
