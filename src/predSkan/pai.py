import pandas as pd
import numpy as np

import sys
sys.path.append('/src')
from src.tools import getFilename

# 数据补零
def dataFillZeros(dataDf2):
    install_date_list = dataDf2['install_date'].unique()
    for install_date in install_date_list:
        df = dataDf2.loc[(dataDf2.install_date == install_date)]
        for i in range(64):
            if df.loc[df.cv == i,'sumr7usd'].sum() == 0 and df.loc[df.cv == i,'count'].sum() == 0:
                dataDf2 = dataDf2.append(pd.DataFrame(data={
                    'install_date':[install_date],
                    'count':[0],
                    'sumr7usd':[0],
                    'cv':[i]
                }),ignore_index=True)
    return dataDf2

# return trainDf and testDf
def getData():
    trainDf = pd.DataFrame()
    testDf = pd.DataFrame()

    df = pd.read_csv(getFilename('totalData%s_%s'%('20220501','20220930')))

    df = dataFillZeros(df)

    trainDf = df.loc[(df.install_date < '2022-09-01')].sort_values(by=['install_date','cv'])
    trainX = trainDf['count'].to_numpy().reshape((-1,64))
    trainSumByDay = trainDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
    trainY = trainSumByDay.to_numpy()
    trainNp = np.hstack((trainX,trainY))
    trainDf = pd.DataFrame(trainNp, columns = ['cv0','cv1','cv2','cv3','cv4','cv5','cv6','cv7','cv8','cv9','cv10','cv11','cv12','cv13','cv14','cv15','cv16','cv17','cv18','cv19','cv20','cv21','cv22','cv23','cv24','cv25','cv26','cv27','cv28','cv29','cv30','cv31','cv32','cv33','cv34','cv35','cv36','cv37','cv38','cv39','cv40','cv41','cv42','cv43','cv44','cv45','cv46','cv47','cv48','cv49','cv50','cv51','cv52','cv53','cv54','cv55','cv56','cv57','cv58','cv59','cv60','cv61','cv62','cv63','sumr7usd'])

    testDf = df.loc[(df.install_date >= '2022-09-01')].sort_values(by=['install_date','cv'])
    testX = testDf['count'].to_numpy().reshape((-1,64))
    testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
    testY = testSumByDay.to_numpy()
    testNp = np.hstack((testX,testY))
    testDf = pd.DataFrame(testNp, columns = ['cv0','cv1','cv2','cv3','cv4','cv5','cv6','cv7','cv8','cv9','cv10','cv11','cv12','cv13','cv14','cv15','cv16','cv17','cv18','cv19','cv20','cv21','cv22','cv23','cv24','cv25','cv26','cv27','cv28','cv29','cv30','cv31','cv32','cv33','cv34','cv35','cv36','cv37','cv38','cv39','cv40','cv41','cv42','cv43','cv44','cv45','cv46','cv47','cv48','cv49','cv50','cv51','cv52','cv53','cv54','cv55','cv56','cv57','cv58','cv59','cv60','cv61','cv62','cv63','sumr7usd'])
    
    return trainDf, testDf

if __name__ == '__main__':
    trainDf, testDf = getData()
    trainDf.to_csv('trainData.csv')
    testDf.to_csv('testData.csv')