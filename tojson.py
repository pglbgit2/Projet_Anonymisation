import json

# dataframe has to respect: idOriginal, Date, AnonymId
# name is not important
def dataframeToJson(dataframe, nbofID, databasename):
    Liste_finale = dataframe.toPandas().values.tolist()
    json_rendu = dict()

    for i in range(0, nbofID - 1):

        identifiant = Liste_finale[i][0]
        date = Liste_finale[i][1]
        iden_Anon = Liste_finale[i][2]

        if  identifiant not in json_rendu:
            json_rendu[identifiant] = dict()

        json_rendu[identifiant][date] = iden_Anon

    json_out = json.dumps(json_rendu)
    with open(databasename+"_Identification.json", "w") as outfile:
        outfile.write(json_out)