import CSVmanager

def correlate(value1, value2):
    assert len(value1) == len(value2)
    nb = 0
    for i in range(len(value1)):
        if value1[i] == value2[i]:
            nb+=1
    return nb
    
def identify(Anonymfilename, Originalfilename):
    anonymTab = CSVmanager.readTabCSVFile(Anonymfilename)
    OgTab = CSVmanager.readTabCSVFile(Originalfilename)
    corrTab = []
    for i in range(1,len(anonymTab)):
        maxNumberOfCorrelation = 0
        lineOfCorrelation = -1
        for j in range(1,len(OgTab)):
            nbCorr = correlate(anonymTab[i], OgTab[j])
            if nbCorr > maxNumberOfCorrelation:
                maxNumberOfCorrelation = nbCorr
                lineOfCorrelation = j
        corrTab.append([i+1,lineOfCorrelation+1]) # +1 parce que c'est des lignes et que les tableaux commencent à 0
    return corrTab

print(identify("anonymisedTab.csv", "tableau.csv")) # format de retour: [ligne du tableau anonyme, ligne du tableau original]