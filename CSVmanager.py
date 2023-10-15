import csv

def writeTabCSVFile(tableau, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for ligne in tableau:
            writer.writerow(ligne)

def readTabCSVFile(filename):
    tableau = []
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for ligne in reader:
            tableau.append(ligne)
    return tableau

