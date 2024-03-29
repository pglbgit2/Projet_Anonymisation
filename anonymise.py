import CSVmanager
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
    for _ in range(nbvalues):
        id_user = generateString(5)
        date_debut = datetime(year=2023, month=1, day=1)
        date_fin = datetime(year=2023, month=12, day=31)
        date = date_debut + timedelta(days=random.randint(0, (date_fin - date_debut).days))
        for _ in range(random.randint(3,12)):
            latitude = random.uniform(3.686, 3.688)
            longitude = random.uniform(43, 44)
            ensemble_de_valeurs.append([id_user, date, latitude, longitude])
    CSVmanager.writeTabCSVFile(ensemble_de_valeurs, filename)

def anonymise(fileToReadName, fileToWriteName):
    tab = CSVmanager.readTabCSVFile(fileToReadName)
    for value in tab:
        value[0] = generateString(5)
        value[2] += random.uniform(0.0001, 0.0002)
        value[3] += random.uniform(0.001, 0.001)
    random.shuffle(tab)
    CSVmanager.writeTabCSVFile(tab,fileToWriteName)



#generate_csv('tableau.csv', 50)
anonymise("tableau.csv", "anonymisedTab.csv")