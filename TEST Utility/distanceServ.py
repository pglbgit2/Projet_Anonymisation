import csv
import sys
import argparse

#################################
#         Global variables      #
# To know:                      #
# dx =1 means that you allow    #
# a maximum of 111.195km        #
# 0.0001 : cellule au mÃ¨tre     #
# 0.001 : cellule Ã  la rue      #
# 0.01 : cellule au quartier    #
# 0.1 : cellule Ã  la ville      #
# 1 : cellule Ã  la rÃ©gion       #
# 10 : cellule au pays          #
#                               #
#################################
dx = 0.1

#################################
#         Function              #
#################################
def calcul_utility(diff):
    score = diff*(-1/dx) + 1
    if(score < 0):
        return 0
    return score

#################################
#         Utiliy Function       #
#################################
def main(nona_reader, anon_reader, parameters={"dx":0.1}):
    global dx
    dx = parameters['dx']
    #variables
    utility = 0
    line_utility= 0
    filesize = 0

    #read the files and calcul
    for lineAno, lineNonAno in zip(nona_reader, anon_reader):
        filesize += 1
        if lineAno[0] != "DEL":
            diff_lat = abs(float(lineNonAno[3])-float(lineAno[3]))
            diff_long = abs(float(lineNonAno[2])-float(lineAno[2]))
            diff = diff_lat + diff_long
            line_utility += calcul_utility(diff)
        else:
            line_utility += 0

    print(filesize)
    print(line_utility)
    utility = line_utility / filesize
    return utility


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.anonymized, args.original))