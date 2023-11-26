import pandas as pd


def writeTabCSVFile(tableau, filename):
    pd.DataFrame.to_csv(pd.DataFrame(tableau),filename,index=False, header=False) 

def readTabCSVFile(filename):
    return pd.read_csv(filename).values

def readCorrectlyCSVFile(filename):
    return pd.read_csv(filename, sep="\t").values
    
def writeCorrectlyCSVFile(tableau, filename):
    pd.DataFrame.to_csv(pd.DataFrame(tableau),filename,index=False, header=True, sep="\t") 