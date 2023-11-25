import dateUtil, distanceServ, hourUtil, meet, POI, tuile, csv
def mesureUtility(OGFile, AnonymFile, separator):
    fd_nona_file = open(OGFile, "r")
    fd_anon_file = open(AnonymFile, "r")
    nona_reader = csv.reader(fd_nona_file, delimiter=separator)
    anon_reader = csv.reader(fd_anon_file, delimiter=separator)
    somme = 0
    somme += dateUtil.main(nona_reader,anon_reader)
    somme += distanceServ.main(nona_reader,anon_reader)
    somme += hourUtil.main(nona_reader,anon_reader)
    somme += meet.main(nona_reader,anon_reader)
    somme += POI.main(nona_reader,anon_reader)
    somme += tuile.main(nona_reader,anon_reader)
    moyenne = somme / 6
    print(moyenne)
    return moyenne