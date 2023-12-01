import csv
import json
import datetime
import argparse

from collections import defaultdict
def dictstruc(): return defaultdict(int)

#/\/\/\/\/\/\ Nom de la mÃ©trique: DÃ©placements effectuÃ©s /\/\/\/\/\/\
#Le but de cette mÃ©trique est de calculer la diffÃ©rence de zone de couverture dâ€™un individu durant les 12 semaines dâ€™Ã©tude.
#Lâ€™idÃ©e est la suivante : la mÃ©trique permet de vÃ©rifier que, globalement, la version anonymisÃ©e garde les informations de dÃ©placement et de couverture dâ€™un individu.
#Pour ce faire on mesure le nombre de cellules diffÃ©rentes dans laquelle lâ€™utilisateur a sÃ©journÃ©.
#Le score est calculÃ© de la maniÃ¨re suivante :

#       Somme [pour chaque i individu] :
#               Si nb_cellule_fichier_original_pour_i > nb_cellule_fichier_anonyme_pour_i :
#                       nb_cellule_fichier_anonyme_pour_i / nb_cellule_fichier_original_pour_i
#               Sinon :
#                       nb_cellule_fichier_original_pour_i / nb_cellule_fichier_anonyme_pour_i
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

##############################
# --- Taille des cellules ---#
##############################
size = 2
#  4 : cellule au mÃ¨tre
#  3 : cellule Ã  la rue
#  2 : cellule au quartier
#  1 : cellule Ã  la ville
#  0 : cellule Ã  la rÃ©gion FranÃ§aise
# -1 : cellule au pays

def main(original_reader, anonymised_reader, parameters={"size":2}, ):
        global size
        size = parameters['size']

       

        tabOri = defaultdict(dictstruc)
        tabAno = defaultdict(dictstruc)
        for lineOri, lineAno in zip(original_reader, anonymised_reader):

                #--- Original file
                id = lineOri[0]
                gps1 = (round(float(lineOri[2]),size), round(float(lineOri[3]),size))
                date = datetime.date.fromisoformat(lineOri[1][0:10])
                calendar = date.isocalendar()
                key = (id)
                #key = (id, calendar[0], calendar[1])
                tabOri[key][gps1] += 1

                #--- Anonymisation file
                if lineAno[0] != "DEL":
                        gps2 = (round(float(lineAno[2]),size), round(float(lineAno[3]),size))
                        tabAno[key][gps2] += 1

        final_tab_original = defaultdict(int)
        final_tab_anonymised = defaultdict(int)
        for id in tabOri:
                final_tab_original[id] = len(tabOri[id])
                final_tab_anonymised[id] = len(tabAno[id])

        total_size = len(final_tab_original)
        score = 0
        for id in final_tab_original:
                if final_tab_original[id]>final_tab_anonymised[id]:
                        score += final_tab_anonymised[id] / final_tab_original[id]
                else:
                        score += final_tab_original[id] / final_tab_anonymised[id]

        return score/total_size

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.original, args.anonymized))