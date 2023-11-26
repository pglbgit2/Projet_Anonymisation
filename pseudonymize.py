import CSVManager
import os
import sys
import string
import random

def pseudonymize(fileToReadName, fileToWriteName):
    tab = CSVManager.readCorrectlyCSVFile(fileToReadName)
    print(tab)
    idAno = {}
    for value in tab:
        if(value[0] not in idAno):
            pseudo = generateString()
            idAno[value[0]] = pseudo
            value[0] = pseudo
        else:
            value[0] = idAno[value[0]]
    CSVManager.writeCorrectlyCSVFile(tab,fileToWriteName)

def generateString():
    size = 7
    autorizedChar = string.ascii_letters + string.digits  
    generatedString = ''.join(random.choice(autorizedChar) for _ in range(size))
    return generatedString

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Utilisation : python3 pseudonymize.py <source.csv> <dest.csv>")
        sys.exit(1)
    pseudonymize(sys.argv[1], sys.argv[2])
