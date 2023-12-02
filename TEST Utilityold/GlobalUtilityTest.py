import dateUtil, distanceServ, hourUtil, meet, POI, tuile, csv
import threading

def mesureUtility(OGFile, AnonymFile, separator):
    results = [0]*6
    threads = []

    def worker(i, func):
        with open(OGFile, "r") as fd_nona_file, open(AnonymFile, "r") as fd_anon_file:
            nona_reader = csv.reader(fd_nona_file, delimiter=separator)
            anon_reader = csv.reader(fd_anon_file, delimiter=separator)
            results[i] = func(nona_reader, anon_reader)

    funcs = [dateUtil.main, distanceServ.main, hourUtil.main, meet.main, POI.main, tuile.main]

    for i, func in enumerate(funcs):
        thread = threading.Thread(target=worker, args=(i, func))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    moyenne = sum(results) / 6
    print(moyenne)
    return moyenne

mesureUtility("../default.csv","./kouign_amann1.csv",'\t')