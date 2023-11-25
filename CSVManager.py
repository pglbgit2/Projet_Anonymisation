import pandas as pd


def writeTabCSVFile(tableau, filename):
    pd.DataFrame.to_csv(pd.DataFrame(tableau),filename,index=False, header=False) 

def readTabCSVFile(filename):
    return pd.read_csv(filename).values