import csv

import CSVmanager


def correlate(value1, value2):
    assert len(value1) == len(value2)
    nb = 0
    for i in range(len(value1)):
        if value1[i] == value2[i]:
            nb += 1
    return nb


import json

# Data to be written
dictionary = {
    "name": "sathiyajith",
    "rollno": 56,
    "cgpa": 8.6,
    "phonenumber": "9976770500"
}

# Serializing json
json_object = json.dumps(dictionary)


# Writing to sample.json
# with open("sample.json", "w") as outfile:
#    outfile.write(json_object)


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


def identification(Anon_file, Original_file):
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
                    for rows in readerAnon:
                        line_Anon = str(rows)
                        line_Anon = line_Anon[2:-2]
                        data_Anon = line_Anon.split("\\t")
                        if date == data_Anon[1] and \
                            float(data_Anon[2])-0.1 <= long <= float(data_Anon[2])+0.1 and \
                            float(data_Anon[3])-0.1 <= lat <= float(data_Anon[3])+0.1:
                            print("Correspondance trouvée")
                            mois_parcouru[identifiant].add(date_du_mois)
                            json_rendu[identifiant][date_du_mois] = rows[0][0]
                            break
                    Anon.seek(0)  # On se remet au début du fichier
                else:
                    continue

            json_out = json.dumps(json_rendu)
            with open("sample.json", "w") as outfile:
                outfile.write(json_out)




identification("autofill_476_clean.csv", "ReferenceINSA.csv")
