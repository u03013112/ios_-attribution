import numpy as np
import datetime
import tensorflow as tf
import pandas as pd

import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.tools import getFilename

from src.predSkan.totalAI2GeoSum2 import geoList,dataStep0,dataStep1,dataStep2,dataStep3,dataStep4,dataStep3_5

def mapeFunc(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100
    
from sklearn import metrics
def testUS():
    modName = '/src/src/predSkan/mod/geo/modUS_mod1__20221103_0903076-42.06.h5'
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = dataStep0('20220901','20221020')
        df = dataStep1('20220901','20221020')
        df2 = dataStep2(df)
        dataDf3 = dataStep3(df2)
        dataDf4 = dataStep4(dataDf3)
        dataDf4.to_csv(getFilename('geoTestData20220901_20221020'))

    # dataDf4 = pd.read_csv(getFilename('geoTestData20220901_20221020'))
    dataDf4 = pd.read_csv(getFilename('totalGeoData4_20220501_20220930'))
    mod = tf.keras.models.load_model(modName)

    testDf = dataDf4.loc[(dataDf4.geo == 'US')].sort_values(by=['install_date','group'])
    testX = testDf['count'].to_numpy().reshape((-1,64))
    testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
    testY = testSumByDay.to_numpy()

    yp = mod.predict(testX)
    print('mape:%.2f%%'%(mapeFunc(testY,yp)))
    r2 = metrics.r2_score(testY,yp)
    print('r2:%.2f%%'%(r2))

    x = np.arange(len(yp))
    plt.title("US ai 2") 
    plt.xlabel("date 2022-09-01~2022-10-20 ") 
    plt.ylabel("true blue,pred red") 
    plt.plot(x,testY.reshape(-1),'b-',label='true')
    plt.plot(x,yp.reshape(-1),'r-',label='pred')
    plt.legend()
    plt.savefig('/src/data/testUS.png')
    print('save pic /src/data/testUS.png')
    plt.clf()
    
def testUSMax():
    modName = '/src/src/predSkan/mod/geo/modUS_mod3__20221104_0801555-36.92.h5'

    dataDf4 = pd.read_csv(getFilename('totalGeoMaxData4_20220501_20220930'))
    mod = tf.keras.models.load_model(modName)

    testDf = dataDf4.loc[(dataDf4.install_date >= '2022-09-01') & (dataDf4.geo == 'US')].sort_values(by=['install_date','group'])
    testX = testDf['count'].to_numpy().reshape((-1,64))
    testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
    testY = testSumByDay.to_numpy()

    yp = mod.predict(testX)
    print('mape:%.2f%%'%(mapeFunc(testY,yp)))
    r2 = metrics.r2_score(testY,yp)
    print('r2:%.2f%%'%(r2))

    x = np.arange(len(yp))
    plt.title("US ai Max") 
    plt.xlabel("date 2022-09-01~2022-10-20 ") 
    plt.ylabel("true blue,pred red") 
    plt.plot(x,testY.reshape(-1),'b-',label='true')
    plt.plot(x,yp.reshape(-1),'r-',label='pred')
    plt.legend()
    plt.savefig('/src/data/testUS.png')
    print('save pic /src/data/testUS.png')
    plt.clf()

def testGeo():
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = dataStep0('20220801','20221020')
        df = dataStep1('20220801','20221020')
        df2 = dataStep2(df)
        dataDf3 = dataStep3(df2)
        dataDf4 = dataStep4(dataDf3)
        dataDf4.to_csv(getFilename('geoTestData20220801_20221020'))
    dataDf4 = pd.read_csv(getFilename('geoTestData20220801_20221020'))

    mods = {
        'US':'modUS_mod3__20221107_0601133-42.36.h5',
        'T1':'modT1_mod3__20221107_0600230-32.63.h5',
        'KR':'modKR_mod3__20221107_0601436-71.52.h5',
        'GCC':'modGCC_mod3__20221107_0600274-62.83.h5',
    }

    for geo in geoList:
        name = geo['name']

        modName = '/src/src/predSkan/mod/geo/' + mods[name]
        mod = tf.keras.models.load_model(modName)

        testDf = dataDf4.loc[(dataDf4.install_date >= '2022-09-01') & (dataDf4.geo == name)].sort_values(by=['install_date','group'])
        testDf = testDf.groupby(['install_date','group']).agg('sum')
        testX = testDf['count'].to_numpy().reshape((-1,64))
        testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
        testY = testSumByDay.to_numpy()
        
        yp = mod.predict(testX)
        print('mape:%.2f%%'%(mapeFunc(testY,yp)))
        r2 = metrics.r2_score(testY,yp)
        print('r2:%.2f%%'%(r2))

        x = np.arange(len(yp))
        plt.title("%s ai"%(name)) 
        plt.xlabel("date 2022-09-01~2022-10-20 ") 
        plt.ylabel("true blue,pred red") 
        plt.plot(x,testY.reshape(-1),'b-',label='true')
        plt.plot(x,yp.reshape(-1),'r-',label='pred')
        plt.legend()
        plt.savefig('/src/data/test%s.png'%(name))
        print('save pic /src/data/test%s.png'%(name))
        plt.clf()


def testGeoSum():
    nList = [3,7,14,28]
    for n in nList:
        
        if __debug__:
            print('debug 模式，并未真的sql')
        else:
            df = dataStep0('20220801','20221020')
            df = dataStep1('20220801','20221020')
            df2 = dataStep2(df)
            dataDf3 = dataStep3(df2)
            dataDf4 = dataStep4(dataDf3)
            dataDf4.to_csv(getFilename('geoSumTestData20220801_20221020'))
            # dataDf4 = pd.read_csv(getFilename('geoSumTestData20220801_20221020'))
            df5 = dataStep3_5(dataDf4,n=n)
            df5.to_csv(getFilename('totalGeoDataSum%d_20220801_20221020'%(n)))
        df5 = pd.read_csv(getFilename('totalGeoDataSum%d_20220801_20221020'%(n)))
    
        mods = {
            'US':{
                # '3':'modSum3_US_mod3__20221107_0900811-34.48.h5',
                # '7':'modSum7_US_mod3__20221107_0901129-35.93.h5',
                # '14':'modSum14_US_mod3__20221107_0900910-36.52.h5',
                # '28':'modSum28_US_mod3__20221107_0901382-35.87.h5',
                '3':'modSum3_US_mod3__20221108_0701834-27.72.h5',
                '7':'modSum7_US_mod3__20221108_0700986-24.50.h5',
                '14':'modSum14_US_mod3__20221108_0702073-26.29.h5',
                '28':'modSum28_US_mod3__20221108_0701094-25.99.h5',
            },
            'T1':{
                '3':'modSum3_T1_mod3__20221107_0900265-27.11.h5',
                '7':'modSum7_T1_mod3__20221107_0900240-19.01.h5',
                '14':'modSum14_T1_mod3__20221107_0900088-16.24.h5',
                '28':'modSum28_T1_mod3__20221107_0900001-8.69.h5',
            },
            'KR':{
                '3':'modSum3_KR_mod3__20221107_0928567-53.49.h5',
                '7':'modSum7_KR_mod3__20221107_0906548-31.54.h5',
                '14':'modSum14_KR_mod3__20221107_0905885-18.75.h5',
                '28':'modSum28_KR_mod3__20221107_0905428-18.05.h5',
            },
            'GCC':{
                '3':'modSum3_GCC_mod3__20221107_0900849-50.72.h5',
                '7':'modSum7_GCC_mod3__20221107_0901275-46.87.h5',
                '14':'modSum14_GCC_mod3__20221107_0900797-25.46.h5',
                '28':'modSum28_GCC_mod3__20221107_0900832-11.14.h5',
            },
        }

        for geo in geoList:
            name = geo['name']

            modName = '/src/src/predSkan/mod/geo/' + mods[name][str(n)]
            mod = tf.keras.models.load_model(modName)

            testDf = df5.loc[(df5.install_date >= '2022-09-01') & (df5.geo == name)].sort_values(by=['install_date','group'])
            testX = testDf['count'].to_numpy().reshape((-1,64))
            testSumByDay = testDf.groupby('install_date').agg(sum=('sumr7usd','sum'))
            testY = testSumByDay.to_numpy()
            
            yp = mod.predict(testX)
            print('mape:%.2f%%'%(mapeFunc(testY,yp)))
            r2 = metrics.r2_score(testY,yp)
            print('r2:%.2f%%'%(r2))

            x = np.arange(len(yp))
            plt.title("%s ai sum%d"%(name,n)) 
            plt.xlabel("date 2022-09-01~2022-10-20 ") 
            plt.ylabel("true blue,pred red") 
            plt.plot(x,testY.reshape(-1),'b-',label='true')
            plt.plot(x,yp.reshape(-1),'r-',label='pred')
            plt.legend()
            plt.savefig('/src/data/test%s%d.png'%(name,n))
            print('save pic /src/data/test%s%d.png'%(name,n))
            plt.clf()

if __name__ == '__main__':
    # testGeo()
    testGeoSum()