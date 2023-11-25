import csv
import argparse

#/\/\/\/\/\/\ Nom de la mÃ©trique: Ecart de l'heure /\/\/\/\/\/\
#Le but de cette mÃ©trique est de calculer l'Ã©cart d'heure pour chaque ligne du fichier anonymisÃ©
#Ainsi, on sâ€™assure de lâ€™authenticitÃ© Ã  la laquelle la position GPS a Ã©tÃ© relevÃ©e.
#Ici, on ne sanctionne pas la modification d'un jour de la semaine. Une position GPS le mardi Ã  16h dÃ©placÃ© le mercredi Ã  16h gardera TOUTE son utilitÃ©
#Le score est calculÃ© de la maniÃ¨re suivante :

#       Chaque ligne vaut 1 point
#       Une fraction de point est enlevÃ©e Ã  chaque heure d'Ã©cart selon le tableau hourdec
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def main(nona_reader, anon_reader, parameters={}): #Compute the utility in function of the date gap
    total = 0
    filesize = 0
    # Set the amount linked to the hour gap
    hourdec = [1, 0.9, 0.8, 0.6, 0.4, 0.2, 0, 0.1, 0.2, 0.3, 0.4, 0.5,
               0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0, 0.2, 0.4, 0.6, 0.8, 0.9]
   
    for row1, row2 in zip(nona_reader, anon_reader):
        if row2[0]=="DEL":
            continue
        score = 1
        filesize += 1
        if len(row2[1]) > 13 and len(row2[0]):
            houranon = int(row2[1][11:13])
            hournona = int(row1[1][11:13])
            if 0 <= houranon < 24 and 0 <= hournona < 24:
                if abs(houranon - hournona):  # Subtract 0,1 points per hour (even if days are identical)
                    score -= hourdec[abs(houranon) - int(hournona)]  # Subtract the amount linked to the hour gap
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
