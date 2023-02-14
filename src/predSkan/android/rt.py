# 尝试找到rt数据差异

import pandas as pd

import sys
sys.path.append('/src')
from src.tools import getFilename

def diff():
    rtDf = pd.read_csv(getFilename('AndroidDataRt20220501_20221215'))
    df = pd.read_csv(getFilename('AndroidData20220501_20221215'))

    
    dataDf = df.groupby(['install_date']).agg('sum')
    dataDf = dataDf.drop(['cv','count','Unnamed: 0'], axis=1)
    dataRtDf = rtDf.groupby(['install_date']).agg('sum')
    dataRtDf = dataRtDf.drop(['cv','count','Unnamed: 0'], axis=1)
    

    retDf = pd.merge(dataDf,dataRtDf,on=['install_date'],suffixes=('', '_rt'))
    retDf['rt1_diff'] = (retDf['sumr1usd'] - retDf['sumr1usd_rt'])/retDf['sumr1usd']
    retDf['rt7_diff'] = (retDf['sumr7usd'] - retDf['sumr7usd_rt'])/retDf['sumr7usd']

    retDf.to_csv(getFilename('AndroidDataRtDiff'))


def diffCv():
    rtDf = pd.read_csv(getFilename('AndroidDataRt20220501_20221215'))
    df = pd.read_csv(getFilename('AndroidData20220501_20221215'))

    dataDf = df.groupby(['install_date','cv']).agg('sum')
    dataDf = dataDf.drop(['sumr1usd','Unnamed: 0'], axis=1)

    dataRtDf = rtDf.groupby(['install_date','cv']).agg('sum')
    dataRtDf = dataRtDf.drop(['sumr1usd','Unnamed: 0'], axis=1)

    retDf = pd.merge(dataDf,dataRtDf,on=['install_date','cv'],suffixes=('', '_rt'))
    retDf['count_diff'] = (retDf['count'] - retDf['count_rt'])/retDf['count']

    retDf.to_csv(getFilename('AndroidDataRtCvDiff'))

diffCv()