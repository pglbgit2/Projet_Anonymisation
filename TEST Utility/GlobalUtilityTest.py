import dateUtil, distanceServ, hourUtil, meet, POI, tuile, csv

def seek_files(fd_OGFile,fd_AnonymFile):
    fd_OGFile.seek(0)
    fd_AnonymFile.seek(0)

def mesureUtility(OGFile, AnonymFile, separator):
    fd_nona_file = open(OGFile, "r")
    fd_anon_file = open(AnonymFile, "r")
    nona_reader = csv.reader(fd_nona_file, delimiter=separator)
    anon_reader = csv.reader(fd_anon_file, delimiter=separator)
    somme = 0
    print("Etape 1")
    somme += dateUtil.main(nona_reader,anon_reader)
    print(somme)
    seek_files(fd_nona_file,fd_anon_file)
    print("Etape 2")
    somme += distanceServ.main(nona_reader,anon_reader)
    print(somme)
    seek_files(fd_nona_file, fd_anon_file)
    print("Etape 3")
    somme += hourUtil.main(nona_reader,anon_reader)
    print(somme)
    seek_files(fd_nona_file, fd_anon_file)
    print("Etape 4")
    somme += meet.main(nona_reader,anon_reader)
    print(somme)
    seek_files(fd_nona_file, fd_anon_file)
    print("Etape 5")
    somme += POI.main(nona_reader,anon_reader)
    print(somme)
    seek_files(fd_nona_file, fd_anon_file)
    print("Etape 6")
    somme += tuile.main(nona_reader,anon_reader)
    moyenne = somme / 6
    print(moyenne)
    return moyenne


mesureUtility("ReferenceINSA.csv","kouign_amann.csv/part-00000-292fbcf7-2257-4f90-bd24-85e3b36eaac3-c000.csv",'\t')