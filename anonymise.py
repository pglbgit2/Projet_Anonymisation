import CSVmanager
from faker import Faker
import random
faker = Faker('fr_FR')

def anonymise(fileToReadName, fileToWriteName):
    tab = CSVmanager.readTabCSVFile(fileToReadName)
    for value in tab:
        value[0] = faker.first_name()
        value[1] = faker.last_name()
    random.shuffle(tab)
    CSVmanager.writeTabCSVFile(tab,fileToWriteName)

def generate_csv(filename, nbvalues):
    ensemble_de_valeurs = []
    for _ in range(nbvalues):
        prenom = faker.first_name()
        nom = faker.last_name()
        annee_naissance = random.randint(1980, 2023)
        mois_naissance = random.randint(1, 12)
        ville_naissance = faker.city()

        ensemble_de_valeurs.append([prenom, nom, annee_naissance, mois_naissance, ville_naissance])
    CSVmanager.writeTabCSVFile(ensemble_de_valeurs, filename)

#generate_csv('tableau.csv', 300)
anonymise("tableau.csv", "anonymisedTab.csv")