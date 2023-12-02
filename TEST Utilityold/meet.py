import csv
import json
import argparse
from collections import OrderedDict


#/\/\/\/\/\/\ Nom de la mÃ©trique: Croisements /\/\/\/\/\/\
#Le but de cette mÃ©trique est d'identifier les cellules oÃ¹ circulent le plus d'utilisateurs.


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
##############################
# --- Minimum de passage  ---#
# Le nombre de localisation  #
# comptÃ©es                                       #
##############################
min_meet = 0
# 0: tout est comptÃ©



def main(original_reader, anonymised_reader, parameters={"size":2, "min_meet":0}):
        global size
        size = parameters['size']
        global min_meet
        min_meet = parameters['min_meet']

        

        tabOri = OrderedDict()
        tabAno = OrderedDict()
        for lineOri, lineAno in zip(original_reader, anonymised_reader):

                #--- Original file
                gps1 = (round(float(lineOri[2]),size), round(float(lineOri[3]),size))
                key = gps1
                #key = ((latitude, longitude))
                if key not in tabOri:
                        tabOri[key] = 0
                else:
                        tabOri[key] += 1

                #--- Anonymisation file
                if lineAno[0] != "DEL":
                        gps2 = (round(float(lineAno[2]),size), round(float(lineAno[3]),size))
                        if gps2 not in tabAno:
                                tabAno[gps2] =0
                        else:
                                tabAno[gps2] +=1

        utility = 0
        total_size = 0
        key_list = list(tabAno.keys())
        tabAno_sorted = sorted(tabAno.items(), key=lambda t: t[1])

        i = min_meet
        if i == 0:
                for key in tabAno:
                        total_size +=1
                        if key in tabOri:
                                if tabAno[key] == 0 or tabOri[key] ==0 or tabOri[key]==tabAno[key]:
                                        score =1
                                else:
                                        score = tabAno[key]/(tabOri[key])
                                if score > 1:
                                        score = 1/score
                                utility+= score
        if i>0:
                tmp = 0
                while tmp < min_meet and tmp < len(tabAno):
                        total_size +=1
                        key = tabAno_sorted[-1-tmp][0]
                        if key in tabOri:
                                if tabAno[key] == 0 or tabOri[key] ==0 or tabOri[key]==tabAno[key]:
                                        score =1
                                else:
                                        score = tabAno[key]/(tabOri[key]+1)
                                if score > 1:
                                        score = 1/score
                                utility+= score
                        tmp +=1

        final_utility = utility/total_size
        #print("---- utility totale : "+str(utility) +"\n")
        #print("UtilitÃ©: "+str(final_utility) +"\n")
        #print("longueur:" +str(total_size)+"\n")
        print(tabAno, tabOri)
        return final_utility

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.original, args.anonymized))