import pandas as pd
# cvMap here
afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')

def getFilename(filename):
    return '/src/data/%s.csv'%(filename)

# 在原有df的基础上加一列来表示
def cvToUSD2(retDf):
    # 列名 usd
    # retDf.loc[:,'usd'] = 0
    retDf.insert(retDf.shape[1],'usd',0)
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        avg = (min_event_revenue + max_event_revenue)/2
        if pd.isna(max_event_revenue):
            avg = 0
        count = retDf.loc[retDf.cv==i,'count']
        retDf.loc[retDf.cv==i,'usd'] = count * avg
    return retDf
