import csv
import json
import datetime
import argparse

from collections import defaultdict
def timedelta_def(): return datetime.timedelta()
def returnnone(): return None
def defaultdicttimedalta(): return defaultdict(timedelta_def)
def defaultdictseption(): return defaultdict(defaultdicttimedalta)
maxdict = lambda dict: max(dict, key=lambda key: dict[key])

#/\/\/\/\/\/\ Nom de la mÃ©trique: Extraction des Points dâ€™IntÃ©rÃªts /\/\/\/\/\/\
#Le but de cette mÃ©trique est de dÃ©tecter les points d'intÃ©rÃªts d'un individu.
#Les points d'intÃ©rÃªts correspondent aux lieux oÃ¹ l'utilisateur a le plus sÃ©journÃ©.
#
#Dans le cadre de ce fichier d'utilitÃ©, nous regardons par dÃ©faut 3 POI les plus importants pour les 12 semaines du dataset.
#1 POI correspond Ã  trois Ã©lÃ©ments: Lieu d'habitation (22h Ã  6h), lieu de travail (9h Ã  16h) et lieu d'activitÃ© (le weekend de 10h Ã  18h)
#
#L'idÃ©e globale de cette utilitÃ© est de s'assurer que l'on retrouve bien dans le fichier anonymisÃ© les lieux clÃ© de la vie d'un individu.
#
#Le score est calculÃ© de la maniÃ¨re suivante :
#       Somme [pour chaque i individu] :
#               Somme [pour chaque POI] :
#                       Si temps_POI_oriFile > temps_POI_anonymFile :
#                               temps_POI_anonymFile / temps_POI_oriFile
#                       Sinon :
#                               temps_POI_oriFile / temps_POI_anonymFile
#Le score est finalement converti sur 1
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

#######################################
# --- Taille des points d'intÃ©rÃªts ---#
#######################################
size = 2
# 4 : cellule au mÃ¨tre
# 3 : cellule Ã  la rue
# 2 : cellule au quartier
# 1 : cellule Ã  la ville
# 0 : cellule Ã  la rÃ©gion
# -1 : cellule au pays

######################################
# --- Nb de POI Ã  vÃ©rifier par ID ---#
######################################
nbPOI = 3
# 3: VÃ©rification des 3 POI les plus frÃ©quentÃ©s en terme de temps de prÃ©sence.

################################
# --- DÃ©finition des heures ---#
################################
# DÃ©tection des POI -nuit, travail et weekend- durant les heures suivantes:
night_start, night_end = 22, 6
# De 22h00 Ã  6h00
work_start, work_end = 9, 16
# De 9h00 Ã  16h00
weekend_start, weekend_end = 10, 18

def getMaxElement(theDict):
        result = defaultdict(timedelta_def)
        for _ in range(nbPOI):
                if len(theDict)==0:
                        break
                key = maxdict(theDict)
                result[key] = theDict[key]
                del theDict[key]
        return result

last_date_original_tab = defaultdict(returnnone)
last_date_anonymised_tab = defaultdict(returnnone)
def diff_time(key, time1, last_date_tab):
    if last_date_tab[key] is None:
        last_date_tab[key] = time1
        return datetime.timedelta()
    else:
        difference = time1 - last_date_tab[key]
        last_date_tab[key] = time1
        #print(difference)
        return difference


def main(original_reader, anonymised_reader, parameters={"size":2,"nbPOI":3,"night_start":22,"night_end":6,"work_start":9,"work_end":16,"weekend_start":10,"weekend_end":18}):
        global size
        size = parameters['size']
        global nbPOI
        nbPOI = parameters['nbPOI']
        global night_start
        night_start = parameters['night_start']
        global night_end
        night_end = parameters['night_end']
        global work_start
        work_start = parameters['work_start']
        global work_end
        work_end = parameters['work_end']
        global weekend_start
        weekend_start = parameters['weekend_start']
        global weekend_end
        weekend_end = parameters['weekend_end']

       

        tabOri = defaultdict(defaultdictseption)
        tabAno = defaultdict(defaultdictseption)

        for lineOri, lineAno in zip(original_reader, anonymised_reader):

                #--- Original file
                id = lineOri[0]
                date_time = datetime.datetime.fromisoformat(lineOri[1][:19])
                #calendar = date_time.date().isocalendar()
                key = (id)
                #key = (id, calendar[0], calendar[1])

                gps = (round(float(lineOri[2]),size), round(float(lineOri[3]),size))
                if date_time.weekday()<5:
                        if date_time.time()>datetime.time(night_start,00) or date_time.time()<datetime.time(night_end,00):
                                tabOri[key]['night'][gps] += diff_time(key, date_time, last_date_original_tab)
                        elif date_time.time()>datetime.time(work_start,00) and date_time.time()<datetime.time(work_end,00):
                                tabOri[key]['work'][gps] += diff_time(key, date_time, last_date_original_tab)
                else:
                        if date_time.time()>datetime.time(weekend_start,00) and date_time.time()<datetime.time(weekend_end,00):
                                tabOri[key]['weekend'][gps] += diff_time(key, date_time, last_date_original_tab)

                #--- Anonymisation file
                if lineAno[0] != "DEL":
                        date_time = datetime.datetime.fromisoformat(lineAno[1][:19])

                        gps = (round(float(lineAno[2]),size), round(float(lineAno[3]),size))
                        if date_time.weekday()<5:
                                if date_time.time()>datetime.time(night_start,00) or date_time.time()<datetime.time(night_end,00):
                                    tabAno[key]['night'][gps] += diff_time(key, date_time, last_date_anonymised_tab)
                                elif date_time.time()>datetime.time(work_start,00) and date_time.time()<datetime.time(work_end,00):
                                    tabAno[key]['work'][gps] += diff_time(key, date_time, last_date_anonymised_tab)
                        else:
                                if date_time.time()>datetime.time(weekend_start,00) and date_time.time()<datetime.time(weekend_end,00):
                                    tabAno[key]['weekend'][gps] += diff_time(key, date_time, last_date_anonymised_tab)

        final_tab_original = defaultdict(defaultdictseption)
        final_tab_anonymised = defaultdict(defaultdictseption)
        #print(tabAno)
        #print(tabOri)
        for id in tabOri:
                for type in tabOri[id]:
                        final_tab_original[id][type] = getMaxElement(tabOri[id][type])
                        final_tab_anonymised[id][type] = getMaxElement(tabAno[id][type])

        total_size = sum((len(final_tab_original[id][type]) for id in final_tab_original for type in final_tab_original[id]))
        score = 0
        #print(final_tab_original)
        #print(final_tab_anonymised)
        

        for id in final_tab_original:
                for type in final_tab_original[id]:
                        for gps in final_tab_original[id][type]:
                                time_second_original = final_tab_original[id][type][gps].total_seconds() if final_tab_original[id][type][gps].total_seconds()>0 else 0
                                time_second_anonymised = final_tab_anonymised[id][type][gps].total_seconds() if final_tab_anonymised[id][type][gps].total_seconds()>0 else 0
                                if time_second_original==0 and time_second_original==0:
                                    continue
                                if time_second_original > time_second_anonymised:
                                    score += time_second_anonymised / time_second_original
                                else:
                                    score += time_second_original / time_second_anonymised
        return score/total_size



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.original, args.anonymized))