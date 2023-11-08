import CSVManager
#from faker import Faker
import random
from datetime import datetime, timedelta
import string

#faker = Faker('fr_FR')

def generateString(longueur):
    autorizedChar = string.ascii_letters + string.digits  
    generatedString = ''.join(random.choice(autorizedChar) for _ in range(longueur))
    return generatedString

def generate_csv(filename, nbvalues):
    ensemble_de_valeurs = []
    date_debut = datetime(year=2023, month=1, day=1)
    date_fin = datetime(year=2023, month=12, day=31)
    for _ in range(nbvalues):
        id_user = generateString(5)
        for _ in range(random.randint(3,12)):
            date = date_debut + timedelta(days=random.randint(0, (date_fin - date_debut).days))
            latitude = random.uniform(3.686, 3.688)
            longitude = random.uniform(43, 44)
            ensemble_de_valeurs.append([id_user, date, latitude, longitude])
    CSVManager.writeTabCSVFile(ensemble_de_valeurs, filename)

def anonymise(fileToReadName, fileToWriteName):
    tab = CSVManager.readTabCSVFile(fileToReadName)
    for value in tab:
        #value[0] = generateString(5)
        value[2] += random.uniform(0.0001, 0.0002)
        value[3] += random.uniform(0.001, 0.001)
    random.shuffle(tab)
    CSVManager.writeTabCSVFile(tab,fileToWriteName)



#generate_csv('tableau.csv', 50)
#anonymise("tableau.csv", "anonymisedTab.csv")