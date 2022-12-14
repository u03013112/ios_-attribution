import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import sys
sys.path.append('/src')

from src.tools import getFilename

def mediaR1R7():
    df = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    mediaGroups = df['media_group'].unique()
    for mediaGroup in mediaGroups:
        mediaDf = df.loc[df.media_group == mediaGroup]
        mediaDf = mediaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','count':'sum'})

        plt.title("media %s r1/r7"%mediaGroup) 
        plt.xlabel("date")
        plt.ylabel("usd")
        mediaDf['sumr1usd'].plot(label='r1usd')
        mediaDf['sumr7usd'].plot(label='r7usd')
        mediaDf['count'].plot(label='count')
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.savefig('/src/data/media_%s.png'%mediaGroup)
        print('save to /src/data/media_%s.png'%mediaGroup)
        plt.clf()
        print(list(mediaDf['sumr1usd'].to_numpy().reshape(-1)))
        print(list(mediaDf['sumr7usd'].to_numpy().reshape(-1)))

mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'applovin','codeList':['applovin_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'unity','codeList':['unityads_int']},
    {'name':'apple','codeList':['Apple Search Ads']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
    {'name':'snapchat','codeList':['snapchat_int']},
    {'name':'unknown','codeList':[]}
]

import tensorflow as tf
def mediaPred():
    modFileNames = {
        'google':'/src/src/predSkan/media/mod/modS14_google00412-68.40.h5',
        'applovin':'/src/src/predSkan/media/mod/modS7_applovin00111-86.87.h5',
        'bytedance':'/src/src/predSkan/media/mod/modS28_bytedance00159-89.31.h5',
        'unity':'/src/src/predSkan/media/mod/modS14_unity00092-43.07.h5',
        'apple':'/src/src/predSkan/media/mod/modS28_apple00403-69.13.h5',
        'facebook':'/src/src/predSkan/media/mod/modS28_facebook00369-56.98.h5',
        'snapchat':'/src/src/predSkan/media/mod/modS14_snapchat00076-96.66.h5',
    }
    dataDf3 = pd.read_csv(getFilename('mediaIdfa2_20220501_20220930'))
    for media in mediaList:
        name = media['name']
        mod = mod = tf.keras.models.load_model(modFileNames[name])

        testDf = dataDf3.loc[
            (dataDf3.install_date >= '2022-09-01') & (dataDf3.media_group == name)
        ].sort_values(by=['install_date','cv'])
        testDf = testDf.groupby(['install_date','cv']).agg('sum')
        testX = testDf['count'].to_numpy().reshape((-1,64))
        testSumByDay = testDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
        tY = testSumByDay['sumr7usd'].to_numpy()
        pY = mod.predict(testX)

        plt.title(name) 
        plt.xlabel("date 2022-09-01~2022-09-30 ") 
    
        plt.plot(tY.reshape(-1),label='real')
        plt.plot(pY.reshape(-1),label='pred')
        plt.legend()
        plt.savefig('/src/data/test%s.png'%name)
        print('save pic /src/data/test%s.png'%name)
        plt.clf()

def dataAddEMA(df,day=3):
    df.insert(df.shape[1],'ema',0)
    df['ema'] = df['sumr7usd'].ewm(span=day).mean()
    return df.reset_index()

def emaTest():
    df = pd.read_csv(getFilename('mediaIdfa2_20220501_20220930'))

    df2 = df.loc[
        (df.install_date >= '2022-05-01') & (df.install_date < '2022-07-01') & (df.media_group == 'google')
    ].sort_values(by=['install_date','cv'])
    df3 = df2.groupby(['install_date']).agg('sum')
    emaDf = dataAddEMA(df3,7)
    emaDf3 = emaDf.loc[(emaDf.install_date >= '2022-06-01') & (emaDf.install_date < '2022-07-01')]
    print(emaDf3)
    emaDf3['ema'].plot(label='after')
    emaDf3['sumr7usd'].plot(label='after r7')

    emaDf = dataAddEMA(df,7)
    emaDf2 = emaDf.loc[
        (emaDf.install_date >= '2022-06-01') & (emaDf.install_date < '2022-07-01') & (emaDf.media_group == 'google')
    ].sort_values(by=['install_date','cv'])
    emaDf3 = emaDf2.groupby(['install_date']).agg('sum')
    print(emaDf3)
    emaDf3['ema'].plot(label='before')
    emaDf3['sumr7usd'].plot(label='before r7')

    plt.title('ema test') 

    plt.legend()
    plt.savefig('/src/data/ema.png')
    print('save pic /src/data/ema.png')
    plt.clf()


def media():
    df = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    maxDf = pd.read_csv(getFilename('mediaIdfaMax200_20220501_20220930'))
    for media in mediaList:
        name = media['name']
        mediaDf = df.loc[df.media_group == name].sort_values(by=['install_date','cv'])
        trainSumByDay = mediaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
        mediaMaxDf = maxDf.loc[maxDf.media_group == name].sort_values(by=['install_date','cv'])
        trainSumByDayMax = mediaMaxDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
        plt.title("media %s r1 r7"%name)
        trainSumByDay['sumr7usd'].plot(label='r7')
        trainSumByDay['sumr1usd'].plot(label='r1')
        trainSumByDayMax['sumr7usd'].plot(label='max200r7')
        trainSumByDayMax['sumr1usd'].plot(label='max200r1')
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.savefig('/src/data/mediaMax_%s.png'%name)
        print('save to /src/data/mediaMax_%s.png'%name)
        plt.clf()


def mediaN():
    dfList = []
    for n in range(1,7):
        df = pd.read_csv(getFilename('mediaIdfaN%d_20220501_20220930'%n))
        df = df.loc[(df.install_date >= '2022-08-01') & (df.install_date <'2022-09-01')]
        dfList.append(df)
    
    for media in mediaList:
        name = media['name']
        for n in range(len(dfList)):
            df = dfList[n]
            mediaDf = df.loc[df.media_group == name].sort_values(by=['install_date','cv'])
            trainSumByDay = mediaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum'})
        
        
            plt.title("media %s N"%name)
            trainSumByDay['sumr1usd'].plot(label='r%d'%(n+1))

        trainSumByDay['sumr7usd'].plot(label='r7')
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.savefig('/src/data/mediaN_%s.png'%name)
        print('save to /src/data/mediaN_%s.png'%name)
        plt.clf()


def mediaCount():
    # 只统计人数，不画图
    df = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    
    for media in mediaList:
        name = media['name']
        mediaDf = df.loc[df.media_group == name].sort_values(by=['install_date','cv'])
        trainSumByDay = mediaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','count':'sum'})
        
        trainSumByDay['count'].plot(label=name)
    plt.title("media user count")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/media_count.png')
    print('save to /src/data/media_count.png')
    plt.clf()

    # df = df.groupby(['media_group','install_date']).agg({'sumr1usd':'sum','sumr7usd':'sum','count':'sum'}).sort_values(by=['media_group','install_date'])
    # df.to_csv('/src/data/media_count.csv')


# Android
def mediaCountA():
    df = pd.read_csv(getFilename('mediaIdfaA_20220501_20220930'))
    
    for media in mediaList:
        name = media['name']
        mediaDf = df.loc[df.media_group == name].sort_values(by=['install_date','cv'])
        trainSumByDay = mediaDf.groupby('install_date').agg({'sumr1usd':'sum','sumr7usd':'sum','count':'sum'})
        
        trainSumByDay['count'].plot(label=name)
    plt.title("media user count")
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.savefig('/src/data/mediaA_count.png')
    print('save to /src/data/mediaA_count.png')
    plt.clf()

    df = df.groupby(['media_group','install_date']).agg({'sumr1usd':'sum','sumr7usd':'sum','count':'sum'}).sort_values(by=['media_group','install_date'])
    df.to_csv('/src/data/mediaA_count.csv')

# 为了快速排查，暂时现针对这几个媒体
mediaList2 = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted']},
]
# 归一化对输入的影响
def mediaStd():
    # 归一化对train和test的影响
    # 归一化之后的input分布
    idfaDf = pd.read_csv(getFilename('mediaIdfa_20220501_20220930'))
    dayList = []
    day0 = datetime.datetime.strptime('20220501','%Y%m%d')
    for i in range(153):
        day = day0 + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        dayList.append(dayStr)
    for media in mediaList2:
        name = media['name']
        print(name)
        # meanDf = pd.read_csv('/src/data/%sMean20220501_20220731.csv'%name)
        # stdDf = pd.read_csv('/src/data/%sStd20220501_20220731.csv'%name)
        # mean = meanDf['mean'].to_numpy()
        # std = stdDf['std'].to_numpy()
        df = idfaDf.loc[idfaDf.media_group == name].sort_values(by=['install_date','cv'])
        df = df.groupby(['install_date','cv']).agg({'count':'sum'})
        xNpArray = df['count'].to_numpy().reshape((-1,64))
        trainXSum = xNpArray.sum(axis=1).reshape(-1,1)
        xNpArray = xNpArray/trainXSum
        min = xNpArray.T.min(axis=1)
        max = xNpArray.T.max(axis=1)

        data = {
            'install_date':dayList
        }
        for i in range(64):
            c = 'x%d'%i
            data[c]=list(xNpArray.T[i])
            c2 = 'xS%d'%i
            xS = np.nan_to_num((xNpArray-min)/(max-min))
            data[c2]=list(xS.T[i])
        xDf = pd.DataFrame(data=data)
        xDf.to_csv('/src/data/media%s.csv'%(name))

        trainXDf = xDf.loc[(xDf.install_date >= '2022-05-01') & (xDf.install_date <= '2022-07-30')]
        testXDf = xDf.loc[(xDf.install_date >= '2022-08-01') & (xDf.install_date <= '2022-09-01')]
        # 每个media画64张图
        
        for i in range(64):
            c = 'xS%d'%i
            plt.title("media %s %s"%(name,c))
            trainX = list(trainXDf[c].to_numpy())
            trainX.sort()
            plt.plot(trainX,label='train')
            testX = list(testXDf[c].to_numpy())
            testX.sort()
            plt.plot(testX,label='test')
            plt.xticks(rotation=45)
            plt.legend(loc='best')
            plt.savefig('/src/data/media%s%s.png'%(name,c))
            print('save to /src/data/media%s%s.png'%(name,c))
            plt.clf()

if __name__ == '__main__':
    # mediaPred()
    # emaTest()
    # mediaCountA()
    # mediaCount()
    # media()
    mediaStd()

