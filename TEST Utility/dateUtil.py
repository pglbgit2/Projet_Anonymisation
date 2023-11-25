from datetime import date
import csv
import argparse

#/\/\/\/\/\/\ Nom de la mÃ©trique: Ecart de la date /\/\/\/\/\/\
#Le but de cette mÃ©trique est de calculer l'Ã©cart de date pour chaque ligne du fichier anonymisÃ©
#Ainsi, on sâ€™assure de lâ€™authenticitÃ© de la date Ã  laquelle la position GPS a Ã©tÃ© relevÃ©e.
#Le score est calculÃ© de la maniÃ¨re suivante :

#       Chaque ligne vaut 1 points
#               1/3 de point est enlevÃ© par jour d'Ã©cart
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def main(nona_reader, anon_reader, parameters={}): #Compute the utility in function of the date gap
    total = 0
    filesize = 0
    
    for row1, row2 in zip(nona_reader, anon_reader):
        if row2[0]=="DEL":
            continue
        score = 1
        filesize += 1
        if len(row2[1]) > 10 and len(row2[0]):
            year_na, month_na, day_na = row1[1][0:10].split("-")
            year_an, month_an, day_an = row2[1][0:10].split("-")
            try :
                #Uses the ISO calendar to get both week and day number
                dateanon = date(int(year_an), int(month_an), int(day_an)).isocalendar()
                datenona = date(int(year_na), int(month_na), int(day_na)).isocalendar()
            except: return (-1, filesize)
            if dateanon[1] == datenona[1]: # Weeks must be the same
                dayanon = dateanon[2]
                daynona = datenona[2]
                if datenona[2] != dateanon[2]:
                    # Subtract 1/3 of a point per weekday
                    score -= min([abs(dayanon - daynona), abs(max((dayanon, daynona)) - min((dayanon, daynona)) + 7)]) / 3
            else: return (-1, filesize)
        else: return (-1, filesize)
        total += max(0, score) if row2[0] != "DEL" else 0
    return total / filesize


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.original, args.anonymized))
