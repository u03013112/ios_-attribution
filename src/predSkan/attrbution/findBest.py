import pandas as pd

import os

# 找到最佳方案
def findBest():
    path = '/src/data/doc/cv'
    logFilename = os.path.join(path,'log3.csv')

    df = pd.read_csv(logFilename)
    min = df['mape'].min()
    print(min)
    minDf = df.loc[df.mape<=min]
    print(minDf)

findBest()